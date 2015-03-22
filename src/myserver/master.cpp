#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <unistd.h>
#include <queue>
#include <map>

#include "server/messages.h"
#include "server/master.h"

#define DEBUG

// constants
static int thread_num = 24;
static int factor = 1.5;

struct Request{
  Request_msg *msg;
  Client_handle client_handle;
};
typedef struct Request Request;

struct Worker{
  Worker_handle worker_handle;
  std::map<int, Client_handle> pending_job;
  int jobs;
};
typedef struct Worker worker;

static struct Request_Queue{
  int next_tag;
  int next_worker_tag;
	int worker_num;
  int cur_worker_num;
  Worker workers[4];
  std::map<Worker_handle, int> workerMap;
  std::queue<Request> request_que;
  bool server_ready;
	bool ifBooting;	
}que;

void start_node(){
	if(que.cur_worker_num < que.worker_num && !que.ifBooting){
  	Request_msg req(que.next_worker_tag);
		std::stringstream convert;
		convert << que.next_worker_tag;		
  	req.set_arg("name", convert.str());
  	request_new_worker_node(req);
		que.next_worker_tag++;
  	DLOG(INFO) << "start  worker:  " << que.next_worker_tag - 1  << std::endl;
		que.ifBooting = true;
	}
}

void destroy_node(int i){
	Worker_handle worker_handle = que.workers[i].worker_handle;
	que.workers[i].jobs = -1;
	if(que.workers[i].pending_job.size() > 0){
		DLOG(INFO) << "Kill a worker with non-zero job" << std::endl;
	}
	que.workers[i].pending_job.clear();
	std::map<Worker_handle, int>::iterator iter;
	iter = que.workerMap.find(worker_handle);
	if(iter != que.workerMap.end()){
		que.workerMap.erase(iter);
	}
	kill_worker_node(worker_handle); 
  que.workers[i].worker_handle = 0;
	que.cur_worker_num--;
  DLOG(INFO) << "Kill  worker  " << worker_handle  << std::endl;
}

void master_node_init(int max_workers, int& tick_period) {
  std::cout << "Master init" << std::endl;

  tick_period = 1;

	// initialization
  que.next_tag = 0;
  que.cur_worker_num = 0;
	que.worker_num = max_workers;
  que.server_ready = false;
  que.next_worker_tag = random();
	que.ifBooting = false;

	for(int i = 0; i < que.worker_num; i++){
		que.workers[i].jobs = -1;
	}
	start_node();
}

void create_queue_request(Request &request, const Request_msg &msg,
                                            Client_handle client_handle){
  request.msg = new Request_msg(msg.get_tag(), msg.get_request_string());
  request.client_handle = client_handle;
}

void assign_job(int worker, Client_handle client_handle, int tag){
  que.workers[worker].jobs++;
  que.workers[worker].pending_job[tag] = client_handle;
}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {
  if(que.server_ready == false) {
    server_init_complete();
    que.server_ready = true;
  }

 // #ifdef DEBUG
  DLOG(INFO) << que.cur_worker_num  << " worker  " << worker_handle  << std::endl;
  //#endif
	int i;
	for(i = 0; i < que.worker_num; i++){
  	if(que.workers[i].jobs == -1){
				que.workers[i].worker_handle = worker_handle;
  			que.workers[i].jobs = 0;
  			que.workerMap[worker_handle] = i;
				break;
		}
	}
	if(i == que.worker_num){
		DLOG(INFO) << "------------------horrible  worker ------------ " << std::endl;	
	}
  #ifdef DEBUG 
  DLOG(INFO) << que.cur_worker_num  << "  " << worker_handle  << std::endl;
  #endif
  que.cur_worker_num += 1;
	que.ifBooting = false; 
} 

void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {
  DLOG(INFO) << "Master received a response from a worker: [" << resp.get_tag() << ":" << resp.get_response() << "]" << std::endl;
  int tag = resp.get_tag();
  int id = que.workerMap[worker_handle];
  #ifdef DEBUG
    if(id >= que.worker_num){
      DLOG(INFO) << "wrong worker id " << id << std::endl;
    }
  #endif
  Client_handle waiting_client = que.workers[id].pending_job[tag];
  send_client_response(waiting_client, resp);
  std::map<int, Client_handle>::iterator iter;
  iter = que.workers[id].pending_job.find(tag);
  if(iter == que.workers[id].pending_job.end()){
    return;
  }
  que.workers[id].pending_job.erase(iter);
  que.workers[id].jobs--;
}

bool check_workload(){
	for(int i = 0; i < que.worker_num; i++){
			if(que.workers[i].jobs != -1 && que.workers[i].jobs < thread_num * factor)
			{
					return true;
			} 
	}
	return false;
}

void send_request(const Request_msg &req, int tag, Client_handle child_handle){
  for(int i = 0; i < que.worker_num; i++){
      if(que.workers[i].jobs != -1 && que.workers[i].jobs < thread_num * factor){
	       assign_job(i, child_handle, tag);
 		  	 Worker_handle worker = que.workers[i].worker_handle;
			   send_request_to_worker(worker, req);
				 break;
			}
  }
}

void schedule(){
		if(que.request_que.size() >= thread_num){
			start_node();
		}	
}

void cache_request(const Request_msg &req, Client_handle client_handle){
  Request cacheRe;
  create_queue_request(cacheRe, req, client_handle);
  que.request_que.push(cacheRe);
  DLOG(INFO) << "Cache req " << req.get_tag() << "  " << req.get_request_string()<< std::endl;
}

void handle_client_request(Client_handle client_handle, const Request_msg& client_req) {
  DLOG(INFO) << "Received request: " << client_req.get_request_string() << std::endl;
  if (client_req.get_arg("cmd") == "lastrequest") {
    Response_msg resp(0);
    resp.set_response("ack");
    send_client_response(client_handle, resp);
    return;
  }

  int tag = que.next_tag++;
  Request_msg worker_req(tag, client_req);
  if(check_workload()){
			send_request(worker_req, tag, client_handle);	
	}else{
			cache_request(worker_req, client_handle);
			schedule();
	}
  return;
}

void lanch_queued_job(){
	Request r = que.request_que.front();
  que.request_que.pop();
  Request_msg req = Request_msg(*r.msg);	
  DLOG(INFO) << "Lauch request from queue" << req.get_request_string() << std::endl;
	send_request(req, req.get_tag(), r.client_handle);	
  delete r.msg;
}

void clean(){
	if(que.cur_worker_num <= 1){
		return;
	}

	for(int i = 0; i < que.worker_num; i++){
		if(que.workers[i].jobs == 0){
			destroy_node(i);	
		}
  	if(que.cur_worker_num <= 1){
   		 return;
  	}
	}  
}

void handle_tick() {
	DLOG(INFO) << "Tick: come in" << que.request_que.size() << std::endl;
	//case 1: queued request and enough ability
	if(que.request_que.size() > 0){
			  while(que.request_que.size() > 0 && check_workload()){
  			  lanch_queued_job();
  			}
				schedule();		
				DLOG(INFO) << "Tick: req queue size:" << que.request_que.size() << std::endl;		
	}else{//case 2: clean unused worker.
		clean();
	}
	DLOG(INFO) << "Tick: finish" << que.request_que.size() << std::endl;	
}

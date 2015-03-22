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

#define WORKER_MAIN 0
#define WORKER_PROJECT 3

#define WORKER_ARBIT -1

// constants
static int thread_num = 24;
static int thread_num_one = 23;
static int factor = 1;
static float threshold = 0.5;

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
	std::map<std::string, Response_msg> prime_cache;
	std::map<int, std::string> prime_cache_req; 
  std::queue<Request> request_que;
	std::queue<Request> project_que;	
  bool server_ready;
	bool ifBooting;
	bool ifProjectWorker;	
}que;

void init_worker_node(int i, Worker_handle worker_handle){
  que.workers[i].worker_handle = worker_handle;
  que.workers[i].jobs = 0;
  que.workerMap[worker_handle] = i;
}

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

void start_project_node(){
	if(!que.ifProjectWorker){
		Request_msg req(que.next_worker_tag);
    std::stringstream convert;
    convert << que.next_worker_tag;
    req.set_arg("name", convert.str());
    request_new_worker_node(req);
    que.next_worker_tag++;
    que.ifProjectWorker = true;
		DLOG(INFO) << "start project worker:  " << que.next_worker_tag - 1  << std::endl;
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
	que.ifProjectWorker = false;

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

void send_priority_request(const Request_msg &req, int tag, Client_handle child_handle, int w){
  assign_job(w, child_handle, tag);
  Worker_handle worker = que.workers[w].worker_handle;
  send_request_to_worker(worker, req);
}

void lanch_queued_project_job(){
  Request r = que.project_que.front();
  que.project_que.pop();
  Request_msg req = Request_msg(*r.msg);
  DLOG(INFO) << "Lauch request from queue" << req.get_request_string() << std::endl;
  send_priority_request(req, req.get_tag(), r.client_handle, WORKER_PROJECT);
  delete r.msg;
}

void handle_project_worker_online(Worker_handle worker_handle){
  que.ifProjectWorker = false;

  init_worker_node(WORKER_PROJECT, worker_handle);

  while(que.project_que.size() > 0){
    lanch_queued_project_job();
  }
}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {
  if(que.server_ready == false) {
    server_init_complete();
    que.server_ready = true;
  }

	if(que.ifProjectWorker == true){
		handle_project_worker_online(worker_handle);
	}else{
  		int i;
  		for(i = 0; i < que.worker_num; i++){
    	if(que.workers[i].jobs == -1){
        init_worker_node(i, worker_handle);
				break;
    	}
  	}
  	if(i == que.worker_num){
   		 DLOG(INFO) << "------------------horrible  worker ------------ " << std::endl;
  	}
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
	std::map<int, std::string>::iterator it;
	it = que.prime_cache_req.find(tag);
	if(it != que.prime_cache_req.end()){
		que.prime_cache[que.prime_cache_req[tag]] = resp; 
	}
  

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
	if(que.workers[WORKER_MAIN].jobs < thread_num_one){
		return true;
	}
	for(int i = 1; i < que.worker_num; i++){
			if(que.workers[i].jobs != -1 && que.workers[i].jobs < thread_num * factor)
			{
					return true;
			} 
	}
	return false;
}

void send_request(const Request_msg &req, int tag, Client_handle child_handle){
	if(que.workers[WORKER_MAIN].jobs < thread_num_one){
         assign_job(WORKER_MAIN, child_handle, tag);
         Worker_handle worker = que.workers[WORKER_MAIN].worker_handle;
         send_request_to_worker(worker, req);
				 return;
	}
  for(int i = 1; i < que.worker_num - 1; i++){
      if(que.workers[i].jobs != -1 && que.workers[i].jobs < thread_num * factor){
	       assign_job(i, child_handle, tag);
 		  	 Worker_handle worker = que.workers[i].worker_handle;
			   send_request_to_worker(worker, req);
				 break;
			}
  }
}

void send_project_request(const Request_msg &req, int tag, Client_handle child_handle){
		if(que.workers[WORKER_PROJECT].jobs != -1){
         assign_job(WORKER_PROJECT, child_handle, tag);
         Worker_handle worker = que.workers[WORKER_PROJECT].worker_handle;
         send_request_to_worker(worker, req);
		}
}

void schedule_worker(){
		if(que.request_que.size() >= thread_num * threshold){
			start_node();
		}	
}

void schedule_request(const Request_msg &req, int tag, Client_handle client_handle){
	send_request(req, tag, client_handle); 
}

void cache_request(const Request_msg &req, Client_handle client_handle){
  Request cacheRe;
  create_queue_request(cacheRe, req, client_handle);
  que.request_que.push(cacheRe);
  DLOG(INFO) << "Cache req " << req.get_tag() << "  " << req.get_request_string()<< std::endl;
}

void cache_project_request(const Request_msg &req, Client_handle client_handle){
  Request cacheRe;
  create_queue_request(cacheRe, req, client_handle);
  que.project_que.push(cacheRe);
  DLOG(INFO) << "Cache project req " << req.get_tag() << "  " << req.get_request_string()<< std::endl;
}

void prime_cache_set(const Response_msg& resp){
	std::string arg = que.prime_cache_req[resp.get_tag()];
	que.prime_cache[arg] = resp;
}

bool prime_cache_find(const Request_msg& client_req){
	std::map<std::string, Response_msg>::iterator it;
	it = que.prime_cache.find(client_req.get_arg("n"));
	if(it != que.prime_cache.end()){
			return true; 
	}
	return false; 
}

void handle_client_request(Client_handle client_handle, const Request_msg& client_req) {
  DLOG(INFO) << "Received request: " << client_req.get_request_string() << std::endl;
  if (client_req.get_arg("cmd") == "lastrequest") {
    Response_msg resp(0);
    resp.set_response("ack");
    send_client_response(client_handle, resp);
  	return;
	}

  if(client_req.get_arg("cmd") == "countprimes"){
    if(prime_cache_find(client_req)){
      Response_msg resp(que.prime_cache[client_req.get_arg("n")]);
      send_client_response(client_handle, resp);
    	return;
		}
  }
	
  int tag = que.next_tag++;
  Request_msg worker_req(tag, client_req);
	
	if(client_req.get_arg("cmd") == "tellmenow"){
		send_priority_request(worker_req, tag, client_handle, WORKER_MAIN);
		goto done;
	}

	if(client_req.get_arg("cmd") == "countprimes"){
		std::string arg = client_req.get_arg("n");
		que.prime_cache_req[tag] = arg;	 
	}

	if(client_req.get_arg("cmd") == "projectidea"){
		if(que.workers[WORKER_PROJECT].jobs != -1){
			send_priority_request(worker_req, tag, client_handle, WORKER_PROJECT);
		
		}else{
		//start project worker
		start_project_node();
		//queue prjectidea req
		cache_project_request(worker_req, client_handle);			
		}	
		goto done;
	}
	
	 if(check_workload()){
		schedule_request(worker_req, tag, client_handle); 	
		//send_request(worker_req, tag, client_handle);	
	 }else{
		cache_request(worker_req, client_handle);
		schedule_worker();
	 }

done:
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

	for(int i = 1; i < que.worker_num; i++){
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
				schedule_worker();		
				DLOG(INFO) << "Tick: req queue size:" << que.request_que.size() << std::endl;		
	}else if(que.project_que.size() > 0){ //case 2: queued project req
    if(que.workers[WORKER_PROJECT].jobs != -1){
      while(que.project_que.size() > 0){
				lanch_queued_project_job();
			}
		}else{
			start_project_node();
		}	
	}
	
	clean();
	DLOG(INFO) << "Tick: finish" << que.request_que.size() << std::endl;	
}

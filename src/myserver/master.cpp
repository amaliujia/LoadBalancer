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

static int thread_num = 24;

struct Request{
  Request_msg *msg;
  Client_handle client_handle;
};
typedef struct Request Request;

struct Job{
  Client_handle client_handle;
};
typedef struct Job Job;

struct Worker{
  Worker_handle worker_handle;
  std::map<int, Client_handle> pending_job;
  int jobs;
};
typedef struct Worker worker;

static struct Request_Queue{
  int next_tag;
  int max_worker_num;
	int worker_num;
  int cur_worker_num;
	int start_assign;
  Worker workers[32];
  std::map<Worker_handle, int> workerMap;
  std::queue<Request> request_que;
  bool server_ready;
}que;

void master_node_init(int max_workers, int& tick_period) {
  std::cout << "Master init" << std::endl;

  tick_period = 0.5;

  que.next_tag = 0;
  que.max_worker_num = max_workers;

  que.cur_worker_num = 0;
	que.worker_num = 0;
  que.server_ready = false;
	que.start_assign = 0;
	que.max_worker_num = max_workers;
  DLOG(INFO) << "max worker  " << max_workers << std::endl;

  int tag = random();
  for(int i = 0; i < max_workers; i++){
    Request_msg req(tag);
		int id = 0;
		std::stringstream convert;
		convert << id;		
    req.set_arg("name", convert.str());
    request_new_worker_node(req);
  }
}

void create_queue_request(Request &request, const Request_msg &msg,
                                            Client_handle client_handle){
  request.msg = new Request_msg(msg.get_tag(), msg.get_request_string());
  request.client_handle = client_handle;
}

void assign_job(int worker, Client_handle client_handle, int tag){
  //que.workers[worker].jobs++;
  que.workers[worker].pending_job[tag] = client_handle;
}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {
  if(que.server_ready == false) {
    server_init_complete();
    que.server_ready = true;
  }

  //#ifdef DEBUG
  DLOG(INFO) << que.cur_worker_num  << " worker  " << worker_handle  << std::endl;
  //#endif
  que.workers[que.cur_worker_num].worker_handle = worker_handle;
  //que.workers[que.cur_worker_num].jobs = 0;
  que.workerMap[worker_handle] = que.cur_worker_num;
  //#ifdef DEBUG 
  DLOG(INFO) << que.cur_worker_num  << "  " << worker_handle  << std::endl;
  //#endif
  que.cur_worker_num += 1;
	que.worker_num += 1;
}


void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {
  DLOG(INFO) << "Master received a response from a worker: [" << resp.get_tag() << ":" << resp.get_response() << "]" << std::endl;
  int tag = resp.get_tag();
  int id = que.workerMap[worker_handle];
  //#ifdef DEBUG
    if(id >= que.worker_num){
      DLOG(INFO) << "wrong worker id " << id << std::endl;
    }
  //#endif
  Client_handle waiting_client = que.workers[id].pending_job[tag];
  send_client_response(waiting_client, resp);
  std::map<int, Client_handle>::iterator iter;
  iter = que.workers[id].pending_job.find(tag);
  if(iter == que.workers[id].pending_job.end()){
    return;
  }
  que.workers[id].pending_job.erase(iter);
  //que.workers[id].jobs--;
}

void handle_client_request(Client_handle client_handle, const Request_msg& client_req) {
  DLOG(INFO) << "Received request: " << client_req.get_request_string() << std::endl;
  if (client_req.get_arg("cmd") == "lastrequest") {
    Response_msg resp(0);
    resp.set_response("ack");
    send_client_response(client_handle, resp);
    return;
  }

	int i = que.start_assign;
  int tag = que.next_tag++;
  Request_msg worker_req(tag, client_req);
  assign_job(i, client_handle, tag);
	Worker_handle worker = que.workers[i].worker_handle;
	send_request_to_worker(worker, worker_req);
	que.start_assign = (que.start_assign + 1) % que.worker_num;	
  /*for(int i = 0; i < que.worker_num; i++){
    if(que.workers[i].jobs < thread_num){
        int tag = que.next_tag++;
        Request_msg worker_req(tag, client_req);
        assign_job(i, client_handle, tag);
        Worker_handle worker = que.workers[i].worker_handle;
        //#ifdef DEBUG
        DLOG(INFO) << "send to this worker:  " << worker << std::endl;
        //#endif
        send_request_to_worker(worker, worker_req);
        return;
    }
  }
  Request cacheRe;
  int tag = que.next_tag++;
  Request_msg worker_req(tag, client_req);
  create_queue_request(cacheRe, worker_req, client_handle);
  que.request_que.push(cacheRe);
  return;*/
}

void lanuchJob(){
  for(int i = 0; i < que.worker_num; i++){
    while(que.workers[i].jobs < thread_num && que.request_que.size() > 0){
      Request r = que.request_que.front();
      que.request_que.pop();
      assign_job(i, r.client_handle, r.msg->get_tag());
      Worker_handle worker = que.workers[i].worker_handle;
      //#ifdef DEBUG
      DLOG(INFO) <<  "launch worker in click: "<< worker  << std::endl;
      //#endif
      send_request_to_worker(worker, *r.msg);
      delete r.msg;
    }
    if(que.request_que.size() == 0){
      break;
    }
  }
}

void handle_tick() {
  if(que.request_que.size() != 0){
      //lanuchJob();
  }
}

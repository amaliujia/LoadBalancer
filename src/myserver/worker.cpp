#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sstream>
#include <glog/logging.h>
#include <vector>
#include <iostream>
#include <semaphore.h>

#include "server/messages.h"
#include "server/worker.h"
#include "tools/cycle_timer.h"
#include "tools/work_queue.h"

static const int max_threads = 24;

WorkQueue<Request_msg> workQueue;
pthread_t threads_id[max_threads];

void create_pthread(pthread_t *tid, pthread_attr_t *attr,
                    void *(* routine)(void *), void *arg)
{
  int rc = 0;
  if((rc = pthread_create(tid, attr, routine, arg)) != 0){
    std::cout << "pthread create fail: " << tid << std::endl;
  }
}

static void create_computeprimes_req(Request_msg& req, int n) {
  std::ostringstream oss;
  oss << n;
  req.set_arg("cmd", "countprimes");
  req.set_arg("n", oss.str());
}

static void execute_compareprimes(const Request_msg& req, Response_msg& resp) {

    int params[4];
    int counts[4];

    params[0] = atoi(req.get_arg("n1").c_str());
    params[1] = atoi(req.get_arg("n2").c_str());
    params[2] = atoi(req.get_arg("n3").c_str());
    params[3] = atoi(req.get_arg("n4").c_str());

    for (int i=0; i<4; i++) {
      Request_msg dummy_req(0);
      Response_msg dummy_resp(0);
      create_computeprimes_req(dummy_req, params[i]);
      execute_work(dummy_req, dummy_resp);
      counts[i] = atoi(dummy_resp.get_response().c_str());
    }

    if (counts[1]-counts[0] > counts[3]-counts[2])
      resp.set_response("There are more primes in first range.");
    else
      resp.set_response("There are more primes in second range.");
}

void *thread_main(void *arg){
	while(true){
		const Request_msg req = workQueue.get_work();
		Response_msg resp(req.get_tag());
  
		DLOG(INFO) << "Worker got request: [" << req.get_tag() << ":" << req.get_request_string() << "]\n";

  	double startTime = CycleTimer::currentSeconds();

  	if (req.get_arg("cmd").compare("compareprimes") == 0) {
   	 execute_compareprimes(req, resp);
  	}else{
   	 execute_work(req, resp);
  	}

  	double dt = CycleTimer::currentSeconds() - startTime;
  	DLOG(INFO) << "Worker completed work in " << (1000.f * dt) << " ms (" << req.get_tag()  << ")\n";
	
		worker_send_response(resp);
		//DLOG(INFO) << "Still remain" << ":" << workQueue.get_size() << "]\n";
	}
  return NULL; 
}

void worker_node_init(const Request_msg& params) {
  DLOG(INFO) << "**** Initializing worker: " << params.get_arg("name") << " ****\n";

  for(int i = 0; i < max_threads; i++){		
		create_pthread(&threads_id[i], NULL, thread_main, NULL);
  }
}

void worker_handle_request(const Request_msg& req) {
	Request_msg r(req);
	workQueue.put_work(r);
}

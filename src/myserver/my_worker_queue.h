#ifndef _MY_WORKER_QUEUE_H
#define _MY_WORKER_QUEUE_H

#include <vector>
#include <glog/logging.h>

template <class T>
class WorkQueue {
private:
  std::vector<T> storage;
  pthread_mutex_t queue_lock;
  pthread_cond_t queue_cond;

public:

  WorkQueue() {
    pthread_cond_init(&queue_cond, NULL);
    pthread_mutex_init(&queue_lock, NULL);
  }

  T get_work() {
    pthread_mutex_lock(&queue_lock);
    while (storage.size() == 0) {
      pthread_cond_wait(&queue_cond, &queue_lock);
    }

    T item = storage.front();
    storage.erase(storage.begin());
    pthread_mutex_unlock(&queue_lock);
    return item;
  }

  void put_work(const T& item) {
    pthread_mutex_lock(&queue_lock);
    storage.push_back(item);
    pthread_mutex_unlock(&queue_lock);
    pthread_cond_signal(&queue_cond);
  }

	void put_work_project(const T& item){
    pthread_mutex_lock(&queue_lock);
   	typename std::vector<T>::iterator iter;
		bool ifFind = false;
		for(iter = storage.begin(); iter != storage.end(); iter++){
			if(iter->get_arg("cmd").compare("projectidea") == 0){
					ifFind = true;
			}else if(iter->get_arg("cmd").compare("projectidea") != 0 && ifFind){
				storage.insert(iter, item);
				DLOG(INFO) << "**** Insert project !! "  << " ****\n";
				break;	
			}	
		}
		
		if(!ifFind){
			storage.push_back(item);
			DLOG(INFO) << "**** Append project !! "  << " ****\n";	
		}	 
    pthread_mutex_unlock(&queue_lock);
    pthread_cond_signal(&queue_cond);
	}
	
	void put_work_front(const T& item){
    pthread_mutex_lock(&queue_lock);
		storage.insert(storage.begin(), item);
    pthread_mutex_unlock(&queue_lock);
    pthread_cond_signal(&queue_cond);
	}
		
};

#endif

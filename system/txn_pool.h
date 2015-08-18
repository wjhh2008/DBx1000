#ifndef _TXN_POOL_H_
#define _TXN_POOL_H_

#include "global.h"
#include "helper.h"

class txn_man;
class base_query;
class row_t;

struct txn_node {
 public:
    txn_man * txn;
    base_query * qry;
    struct txn_node * next;
};

typedef txn_node * txn_node_t;


class TxnPool {
public:
  void init();
  bool empty(uint64_t node_id);
  void add_txn(uint64_t node_id, txn_man * txn, base_query * qry);
  txn_man * get_txn(uint64_t node_id, uint64_t txn_id);
  base_query * get_qry(uint64_t node_id, uint64_t txn_id);
  void restart_txn(uint64_t txn_id);
  void delete_txn(uint64_t node_id, uint64_t txn_id);
  uint64_t get_min_ts(); 

  void spec_next(uint64_t tid);
  void start_spec_ex(uint64_t tid);
  void end_spec_ex();
  void commit_spec_ex(int r,uint64_t tid);

  //uint64_t inflight_cnt;
  bool spec_mode[PART_CNT/NODE_CNT];

private:
	uint64_t _node_id;

  pthread_mutex_t mtx;
  pthread_cond_t cond_m;
  pthread_cond_t cond_a;
  volatile bool modify;
  int access;


  txn_node_t *txns;
};

#endif

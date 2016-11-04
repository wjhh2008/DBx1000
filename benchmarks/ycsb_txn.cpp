#include "global.h"
#include "helper.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "query.h"

void ycsb_txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	txn_man::init(h_thd, h_wl, thd_id);
	_wl = (ycsb_wl *) h_wl;
}

RC ycsb_txn_man::run_txn(base_query * query) {
	RC rc;
	ycsb_query * m_query = (ycsb_query *) query;
	ycsb_wl * wl = (ycsb_wl *) h_wl;
	itemid_t * m_item = NULL;
  	row_cnt = 0;
#ifdef RCC
    pred_cnt = 0;
#endif

	for (uint32_t rid = 0; rid < m_query->request_cnt; rid ++) {
		ycsb_request * req = &m_query->requests[rid];
		int part_id = wl->key_to_part( req->key );
		bool finish_req = false;
		UInt32 iteration = 0;
#ifdef RCC
        access_t type = req->rtype;
        Predicate * pred = NULL;
        if (type == SCAN) {
            if (predicates[pred_cnt] == NULL)
              predicates[pred_cnt] = (Predicate *) _mm_malloc(sizeof(Predicate), 64);
            pred = predicates[pred_cnt];
            pred_cnt++;
            pred->st_row_key = req->key;
            pred->ts = glob_manager->get_last_txn();
        }
#endif
		while ( !finish_req ) {
			if (iteration == 0) {
				m_item = index_read(_wl->the_index, req->key, part_id);
			} 
#if INDEX_STRUCT == IDX_BTREE
			else {
				_wl->the_index->index_next(get_thd_id(), m_item);
				if (m_item == NULL)
					break;
			}
#elif INDEX_STRUCT == IDX_HASH
            else {
                if (req->key + iteration >= SYNTH_TABLE_SIZE)
                  break;
                m_item = index_read(_wl->the_index, req->key + iteration, part_id);
            }
#endif
			row_t * row = ((row_t *)m_item->location);
            row_t * row_local;
#ifndef RCC
			access_t type = req->rtype;
#endif
#ifdef RCC
            if (type == SCAN)
              row_local = row;
            else
#endif
			row_local = get_row(row, type);
			if (row_local == NULL) {
				rc = Abort;
				goto final;
			}

			// Computation //
			// Only do computation when there are more than 1 requests.
            if (m_query->request_cnt > 1) {
                if (req->rtype == RD || req->rtype == SCAN) {
//                  for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
						int fid = 0;
						char * data = row_local->get_data();
						__attribute__((unused)) uint64_t fval = *(uint64_t *)(&data[fid * 10]);
//                  }
                } else {
                    assert(req->rtype == WR);
//					for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
						int fid = 0;
                        char * data = row_local->get_data();
						*(uint64_t *)(&data[fid * 10]) = 0;
//					}
                } 
            }


			iteration ++;
			if (req->rtype == RD || req->rtype == WR || iteration == req->scan_len)
				finish_req = true;
		}
#ifdef RCC
        if (type == SCAN) {
            ASSERT(m_item != NULL);
            pred->ed_row_key = ((row_t *)m_item->location)->get_primary_key();
        }
#endif
	}
	rc = RCOK;
final:
	rc = finish(rc);
	return rc;
}


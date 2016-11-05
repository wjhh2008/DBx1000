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
        UInt64  st_ut_id = glob_manager->get_unit_id(req->key);
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
			access_t type = req->rtype;
#ifdef RCC
            if (type == SCAN)
            {
              row_local = row;
            }
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
        if (req->rtype == SCAN) {
            ASSERT(m_item != NULL);
            UInt64 last_primary_key = ((row_t *)m_item->location)->get_primary_key();
            UInt64  ed_ut_id = glob_manager->get_unit_id(last_primary_key);
            ASSERT(ed_ut_id >= st_ut_id);
            for (UInt64 ut_id = st_ut_id; ut_id <= ed_ut_id; ut_id++) {
                Predicate * pred = get_new_pred();
                pred->ut_id = ut_id;
                pred->ts = glob_manager->get_rcc_unit(ut_id)->get_last_txn();
                pred->st_row_key = ut_id * UNIT_LEN;
                pred->ed_row_key = (ut_id + 1) * UNIT_LEN - 1;
                if (ut_id == st_ut_id)
                    pred->st_row_key = req->key;
                if (ut_id == ed_ut_id)
                  pred->ed_row_key = last_primary_key;
                ASSERT(pred->ed_row_key >= pred->st_row_key);
                pred->whole_unit = (pred->ed_row_key - pred->st_row_key == UNIT_LEN - 1);
            }
        }
#endif
	}
	rc = RCOK;
final:
	rc = finish(rc);
	return rc;
}


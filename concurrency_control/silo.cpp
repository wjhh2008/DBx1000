#include "txn.h"
#include "row.h"
#include "row_silo.h"

#if CC_ALG == SILO

RC
txn_man::validate_silo()
{
	RC rc = RCOK;
	// lock write tuples in the primary key order.
	int write_set[wr_cnt];
	int cur_wr_idx = 0;
#if ISOLATION_LEVEL != REPEATABLE_READ
	int read_set[row_cnt - wr_cnt];
	int cur_rd_idx = 0;
#endif
	for (int rid = 0; rid < row_cnt; rid ++) {
		if (accesses[rid]->type == WR)
			write_set[cur_wr_idx ++] = rid;
#if ISOLATION_LEVEL != REPEATABLE_READ
		else 
			read_set[cur_rd_idx ++] = rid;
#endif
	}

#ifdef RCC
    UInt64 last_ut_id = UINT64_MAX;
#endif

	// bubble sort the write_set, in primary key order 
	for (int i = wr_cnt - 1; i >= 1; i--) {
		for (int j = 0; j < i; j++) {
			if (accesses[ write_set[j] ]->orig_row->get_primary_key() > 
				accesses[ write_set[j + 1] ]->orig_row->get_primary_key())
			{
				int tmp = write_set[j];
				write_set[j] = write_set[j+1];
				write_set[j+1] = tmp;
			}
		}
	}

	int num_locks = 0;
	ts_t max_tid = 0;
	bool done = false;
	if (_pre_abort) {
		for (int i = 0; i < wr_cnt; i++) {
			row_t * row = accesses[ write_set[i] ]->orig_row;
			if (row->manager->get_tid() != accesses[write_set[i]]->tid) {
				rc = Abort;
				goto final;
			}	
		}	
#if ISOLATION_LEVEL != REPEATABLE_READ
		for (int i = 0; i < row_cnt - wr_cnt; i ++) {
			Access * access = accesses[ read_set[i] ];
			if (access->orig_row->manager->get_tid() != accesses[read_set[i]]->tid) {
				rc = Abort;
				goto final;
			}
		}
#endif
	}


	// lock all rows in the write set.
	if (_validation_no_wait) {
		while (!done) {
			num_locks = 0;
			for (int i = 0; i < wr_cnt; i++) {
				row_t * row = accesses[ write_set[i] ]->orig_row;
				if (!row->manager->try_lock())
					break;
				row->manager->assert_lock();
				num_locks ++;
				if (row->manager->get_tid() != accesses[write_set[i]]->tid)
				{
					rc = Abort;
					goto final;
				}
			}
			if (num_locks == wr_cnt)
				done = true;
			else {
				for (int i = 0; i < num_locks; i++)
					accesses[ write_set[i] ]->orig_row->manager->release();
				if (_pre_abort) {
					num_locks = 0;
					for (int i = 0; i < wr_cnt; i++) {
						row_t * row = accesses[ write_set[i] ]->orig_row;
						if (row->manager->get_tid() != accesses[write_set[i]]->tid) {
							rc = Abort;
							goto final;
						}	
					}	
#if ISOLATION_LEVEL != REPEATABLE_READ
					for (int i = 0; i < row_cnt - wr_cnt; i ++) {
						Access * access = accesses[ read_set[i] ];
						if (access->orig_row->manager->get_tid() != accesses[read_set[i]]->tid) {
							rc = Abort;
							goto final;
						}
					}
#endif
				}
#ifdef RCC
                else
                  ASSERT(false);
#endif
				usleep(1);
			}
		}
	} else {
#ifdef RCC
        ASSERT(false);
#endif
		for (int i = 0; i < wr_cnt; i++) {
			row_t * row = accesses[ write_set[i] ]->orig_row;
			row->manager->lock();
			num_locks++;
			if (row->manager->get_tid() != accesses[write_set[i]]->tid) {
				rc = Abort;
				goto final;
			}
		}
	}
#ifdef RCC
    pre_commit_ts = get_sys_clock();
    for (int i = 0; i < wr_cnt; i++) {
        row_t * row = accesses[ write_set[i] ]->orig_row;
        UInt64 ut_id = glob_manager->get_unit_id(row->get_primary_key());
        if (ut_id != last_ut_id) {
            last_ut_id = ut_id;
            glob_manager->get_rcc_unit(ut_id)->add_recent_txn(this);
        }
    }


#endif

	// validate rows in the read set
#if ISOLATION_LEVEL != REPEATABLE_READ
	// for repeatable_read, no need to validate the read set.
	for (int i = 0; i < row_cnt - wr_cnt; i ++) {
		Access * access = accesses[ read_set[i] ];
		bool success = access->orig_row->manager->validate(access->tid, false);
		if (!success) {
			rc = Abort;
			goto final;
		}
		if (access->tid > max_tid)
			max_tid = access->tid;
	}

#ifdef RCC
    for (uint64_t i = 0; i < pred_cnt; i++) {
        Predicate * pred = predicates[i];
        bool success = glob_manager->get_rcc_unit(pred->ut_id)->validate_txn(pred, pre_commit_ts);
        if (!success) {
            rc = Abort;
            goto final;
        }
    }
#endif

#endif
	// validate rows in the write set
	for (int i = 0; i < wr_cnt; i++) {
		Access * access = accesses[ write_set[i] ];
		bool success = access->orig_row->manager->validate(access->tid, true);
		if (!success) {
			rc = Abort;
			goto final;
		}
		if (access->tid > max_tid)
			max_tid = access->tid;
	}
	if (max_tid > _cur_tid)
		_cur_tid = max_tid + 1;
	else 
		_cur_tid ++;
final:
	if (rc == Abort) {
		for (int i = 0; i < num_locks; i++) 
			accesses[ write_set[i] ]->orig_row->manager->release();
        cleanup(rc);
	} else {
		for (int i = 0; i < wr_cnt; i++) {
			Access * access = accesses[ write_set[i] ];
			access->orig_row->manager->write( 
				access->data, _cur_tid );
			accesses[ write_set[i] ]->orig_row->manager->release();
		}        
        cleanup(rc);
	}
	return rc;
}
#endif

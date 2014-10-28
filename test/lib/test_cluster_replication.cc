/**
 *	test_cluster_replication.cc
 *
 *	@author Masanori Yoshimoto <masanori.yoshimoto@gree.net>
 */

#include "app.h"
#include "stats.h"
#include "cluster.h"
#include "cluster_replication.h"
#include "key_resolver_modular.h"
#include "server.h"
#include "thread.h"
#include "thread_handler.h"
#include "thread_pool.h"

#include "mock_cluster.h"
#include "mock_storage.h"
#include "mock_op_proxy_write.h"
#include "connection_iostream.h"

#include <cppcutter.h>

using namespace std;
using namespace gree::flare;

namespace test_cluster_replication {
	class handler_async_response : public thread_handler {
		public:
			vector<string>		requests;
			vector<string>		values;
		private:
			server*						_server;
		public:
			handler_async_response(shared_thread t, server* s):
					thread_handler(t) {
				this->_server = s;
			}

			~handler_async_response() {
			}

			virtual int run() {
				sleep(1);
				while (!this->_thread->is_shutdown_request()) {
					vector<shared_connection_tcp> cs = this->_server->wait();
					if (cs.size() > 0) {
						char *p, *q;
						int len = cs[0]->readline(&p);
						if (len > 0) {
							cs[0]->readline(&q);
							cs[0]->writeline("STORED");
							this->requests.push_back(string(p));
							this->values.push_back(string(q));
							delete[] p;
							delete[] q;
						}
					} else {
						break;
					}
				}
				return 0;
			}
	};

	void sa_usr1_handler(int sig) {
		// just ignore
	}

	int										port;
	mock_cluster*					cl;
	mock_storage*					st;
	server*								s;
	thread_pool*					tp;
	vector<shared_connection_tcp>		cs;
	cluster_replication*	cl_repl;
	struct sigaction	prev_sigusr1_action;

	void setup() {
		struct sigaction sa;
		memset(&sa, 0, sizeof(sa));
		sa.sa_handler = sa_usr1_handler;
		if (sigaction(SIGUSR1, &sa, &prev_sigusr1_action) < 0) {
			log_err("sigaction for %d failed: %s (%d)", SIGUSR1, util::strerror(errno), errno);
			return;
		}

		stats_object = new stats();
		stats_object->update_timestamp();

		port = rand() % (65535 - 1024) + 1024;
		s = new server();

		cl = new mock_cluster("localhost", port);
		tp = new thread_pool(5);
		st = new mock_storage("", 0, 0);
		st->open();

		cl_repl = new cluster_replication(tp);
	}

	void teardown() {
		for (int i = 0; i < cs.size(); i++) {
			cs[i]->close();
		}
		cs.clear();
		s->close();
		tp->shutdown();
		st->close();

		delete cl_repl;
		delete s;
		delete tp;
		delete st;
		delete cl;
		delete stats_object;

		if (sigaction(SIGUSR1, &prev_sigusr1_action, NULL) < 0) {
			log_err("sigaction for %d failed: %s (%d)", SIGUSR1, util::strerror(errno), errno);
			return;
		}
	}

	void prepare_storage(int item_num, int version) {
		for (int i = 0; i < item_num; i++) {
			string key = "key" + boost::lexical_cast<string>(i);
			string data = "VALUE";
			st->set_helper(key, data.c_str(), 0, version);
		}
	}

	storage::entry get_entry(string firstline, storage::parse_type type, string value) {
		storage::entry e;
		e.parse(firstline.c_str(), type);
		shared_byte data(new uint8_t[e.size]);
		memcpy(data.get(), value.c_str(), e.size);
		e.data = data;
		return e;
	}

	void assert_variable(string server_name, int server_port, int concurrency, bool sync = false) {
		cut_assert_equal_string(server_name.c_str(), cl_repl->get_server_name().c_str());
		cut_assert_equal_int(server_port, cl_repl->get_server_port());
		cut_assert_equal_int(concurrency, cl_repl->get_concurrency());
		cut_assert_equal_boolean(sync, cl_repl->get_sync());
	}

	void assert_state(bool started, bool dump) {
		cut_assert_equal_boolean(started, cl_repl->is_started());
		thread_pool::local_map m_cl = tp->get_active(thread_pool::thread_type_cluster_replication);
		if (started) {
			cut_assert_equal_int(cl_repl->get_concurrency(), m_cl.size());
		} else {
			cut_assert_equal_int(0, m_cl.size());
		}
		thread_pool::local_map m_dump = tp->get_active(thread_pool::thread_type_dump_replication);
		if (dump) {
			cut_assert_equal_int(1, m_dump.size());
		} else {
			cut_assert_equal_int(0, m_dump.size());
		}
	}

	void assert_queue_size(int exp) {
		int queue_size = 0;
		thread_pool::local_map m = tp->get_active(thread_pool::thread_type_cluster_replication);
		for (thread_pool::local_map::iterator it = m.begin(); it != m.end(); it++) {
			queue_size += it->second->get_thread_info().queue_size;
		}
		cut_assert_equal_int(exp, queue_size);
	}

	void test_default_value() {
		assert_variable("", 0, 0);
		assert_state(false, false);
	}

	void test_set_sync() {
		cut_assert_equal_boolean(false, cl_repl->get_sync());
		cut_assert_equal_int(0, cl_repl->set_sync(true));
		cut_assert_equal_boolean(true, cl_repl->get_sync());
		cut_assert_equal_int(0, cl_repl->set_sync(false));
		cut_assert_equal_boolean(false, cl_repl->get_sync());
	}

	void test_start_success_when_master() {
		// prepare
		s->listen(port);
		cluster::node master = cl->set_node("localhost", port, cluster::role_master, cluster::state_active);
		cl->set_partition(0, master);
		st->iter_wait = 2 * 1000 * 1000; // 2 secs

		// execute
		cut_assert_equal_int(0, cl_repl->start("localhost", port, 3, st, cl));
		sleep(1);  // waiting for connection establishment

		// assert
		assert_variable("localhost", port, 3);
		assert_state(true, true);
		sleep(2);  // waiting for dump replication completed
		assert_state(true, false);
	}

	void test_start_success_when_slave() {
		// prepare
		s->listen(port);
		cluster::node master = cl->set_node("dummy", port + 1, cluster::role_master, cluster::state_active);
		cluster::node slave = cl->set_node("localhost", port, cluster::role_slave, cluster::state_active);
		cluster::node slaves[] = {slave};
		cl->set_partition(0, master, slaves, 1);
		st->iter_wait = 2 * 1000 * 1000; // 2 secs

		// execute
		cut_assert_equal_int(0, cl_repl->start("localhost", port, 3, st, cl));
		sleep(1);  // waiting for connection establishment

		// assert
		assert_variable("localhost", port, 3);
		assert_state(true, true);
		sleep(2);  // waiting for dump replication completed
		assert_state(true, false);
	}

	void test_start_failure_when_proxy() {
		// prepare
		s->listen(port);
		cl->set_node("localhost", port, cluster::role_proxy, cluster::state_active);

		// execute
		cut_assert_equal_int(-1, cl_repl->start("localhost", port, 1, st, cl));

		// assert
		assert_variable("", 0, 0);
		assert_state(false, false);
	}

	void test_start_success_with_dump() {
		// prepare
		s->listen(port);
		cluster::node master = cl->set_node("localhost", port, cluster::role_master, cluster::state_active);
		cl->set_partition(0, master);
		prepare_storage(3, 1);

		// execute
		cut_assert_equal_int(0, cl_repl->start("localhost", port, 3, st, cl));
		sleep(1);
		assert_variable("localhost", port, 3);
		assert_state(true, true);

		cs = s->wait();
		for (int i = 0; i < 3; i++) {
			sleep(1);
			char *p, *q;
			cs[0]->readline(&p);
			cs[0]->readline(&q);
			cs[0]->writeline("STORED");
			string req = "set key" + boost::lexical_cast<string>(i) + " 0 0 5 1\n";
			cut_assert_equal_string(req.c_str(), p);
			cut_assert_equal_string("VALUE\n", q);
		}
		sleep(1);
		assert_state(true, false);
	}

	void test_start_failure_with_no_concurrency() {
		// prepare
		s->listen(port);
		cluster::node master = cl->set_node("localhost", port, cluster::role_master, cluster::state_active);
		cl->set_partition(0, master);

		// execute
		cut_assert_equal_int(-1, cl_repl->start("localhost", port, 0, st, cl));

		// assert
		assert_variable("", 0, 0);
		assert_state(false, false);
	}

	void test_start_failure_in_started_state() {
		// prepare
		s->listen(port);
		cluster::node master = cl->set_node("localhost", port, cluster::role_master, cluster::state_active);
		cl->set_partition(0, master);
		st->iter_wait = 2 * 1000 * 1000; // 2 secs

		cut_assert_equal_int(0, cl_repl->start("localhost", port, 2, st, cl));
		sleep(1);  // waiting for connection establishment

		// execute
		cut_assert_equal_int(-1, cl_repl->start("localhost", port, 4, st, cl));

		// assert
		assert_variable("localhost", port, 2);
		assert_state(true, true);
	}

	void test_stop_success_in_stated_state() {
		// prepare
		s->listen(port);
		cluster::node master = cl->set_node("localhost", port, cluster::role_master, cluster::state_active);
		cl->set_partition(0, master);
		st->iter_wait = 3 * 1000 * 1000; // 3 secs

		cut_assert_equal_int(0, cl_repl->start("localhost", port, 3, st, cl));
		sleep(1);  // waiting for connection establishment
		assert_variable("localhost", port, 3);
		assert_state(true, true);

		// execute
		cut_assert_equal_int(0, cl_repl->stop());
		sleep(1);  // waiting for shutdown of asynchronous replication threads.

		// assert
		assert_variable("", 0, 0);
		assert_state(false, false);
	}

	void test_stop_sucess_in_not_started_state() {
		// prepare
		s->listen(port);
		cluster::node master = cl->set_node("localhost", port, cluster::role_master, cluster::state_active);
		cl->set_partition(0, master);

		// execute
		cut_assert_equal_int(0, cl_repl->stop());

		// assert
		assert_variable("", 0, 0);
		assert_state(false, false);
	}

	void test_on_pre_proxy_read_success() {
		// prepare
		s->listen(port);
		cluster::node master = cl->set_node("localhost", port, cluster::role_master, cluster::state_active);
		cl->set_partition(0, master);
		st->iter_wait = 3 * 1000 * 1000; // 3 secs

		cut_assert_equal_int(0, cl_repl->start("localhost", port, 3, st, cl));
		sleep(1);  // waiting for connection establishment
		assert_variable("localhost", port, 3);
		assert_state(true, true);

		// execute
		shared_connection c(new connection_sstream(" TEST"));
		op_get op(c, cl, st);
		cut_assert_equal_int(0, cl_repl->on_pre_proxy_read(&op));
	}

	void test_on_pre_proxy_write_success() {
		// prepare
		s->listen(port);
		cluster::node master = cl->set_node("localhost", port, cluster::role_master, cluster::state_active);
		cl->set_partition(0, master);
		st->iter_wait = 3 * 1000 * 1000; // 3 secs

		cut_assert_equal_int(0, cl_repl->start("localhost", port, 3, st, cl));
		sleep(1);  // waiting for connection establishment
		assert_variable("localhost", port, 3);
		assert_state(true, true);

		// execute
		shared_connection c(new connection_sstream(" TEST 0 0 5\r\nVALUE\r\n"));
		op_set op(c, cl, st);
		cut_assert_equal_int(0, cl_repl->on_pre_proxy_write(&op));
	}

	void test_on_post_proxy_write_failure_in_not_started_state() {
		// prepare
		s->listen(port);
		cluster::node master = cl->set_node("localhost", port, cluster::role_master, cluster::state_active);
		cl->set_partition(0, master);

		// execute
		shared_connection c(new connection_sstream(" TEST 0 0 5\r\nVALUE\r\n"));
		op_set op(c, cl, st);
		cut_assert_equal_int(-1, cl_repl->on_post_proxy_write(&op));
	}

	void test_on_post_proxy_write_success_when_async() {
		// prepare
		s->listen(port);
		cluster::node master = cl->set_node("localhost", port, cluster::role_master, cluster::state_active);
		cl->set_partition(0, master);

		cut_assert_equal_int(0, cl_repl->start("localhost", port, 1, st, cl));
		sleep(1);  // waiting for connection establishment and dump completed
		assert_variable("localhost", port, 1);
		assert_state(true, false);

		// response asynchronously because it's failed to flush response
		// to connection stream on this thread.
		shared_thread t = tp->get(thread_pool::thread_type_request);
		handler_async_response* h = new handler_async_response(t, s);
		t->trigger(h);

		// execute
		for (int i = 0; i < 5; i++) {
			shared_connection c(new connection_sstream("dummy"));
			mock_op_proxy_write op(c, cl, st);
			string key = "key" + boost::lexical_cast<string>(i);
			storage::entry e = get_entry(" " + key + " 0 0 5 3", storage::parse_type_set, "VALUE");
			op.set_entry(e);

			cut_assert_equal_int(0, cl_repl->on_post_proxy_write(&op));
		}

		// assert
		assert_queue_size(5);
		sleep(5);  // waiting for queue proceeded
		assert_queue_size(0);
		cut_assert_equal_int(5, h->requests.size());
		cut_assert_equal_int(5, h->values.size());
		for (int i = 0; i < 5; i++) {
			string ext_req = "set key" + boost::lexical_cast<string>(i) + " 0 0 5 3\n";
			cut_assert_equal_string(ext_req.c_str(), h->requests[i].c_str());
			cut_assert_equal_string("VALUE\n", h->values[i].c_str());
		}
	}

	void test_on_post_proxy_write_success_with_sync() {
		// prepare
		s->listen(port);
		cluster::node master = cl->set_node("localhost", port, cluster::role_master, cluster::state_active);
		cl->set_partition(0, master);
		cl_repl->set_sync(true);

		cut_assert_equal_int(0, cl_repl->start("localhost", port, 1, st, cl));
		sleep(1);
		assert_variable("localhost", port, 1, true);
		assert_state(true, false);

		shared_thread t = tp->get(thread_pool::thread_type_request);
		handler_async_response* h = new handler_async_response(t, s);
		t->trigger(h);

		// execute
		for (int i = 0; i < 5; i++) {
			shared_connection c(new connection_sstream("dummy"));
			mock_op_proxy_write op(c, cl, st);
			string key = "key" + boost::lexical_cast<string>(i);
			storage::entry e = get_entry(" " + key + " 0 0 5 3", storage::parse_type_set, "VALUE");
			op.set_entry(e);

			cut_assert_equal_int(0, cl_repl->on_post_proxy_write(&op));
			assert_queue_size(0);
			cut_assert_equal_int(i + 1, h->requests.size());
			cut_assert_equal_int(i + 1, h->values.size());
			string ext_req = "set " + key + " 0 0 5 3\n";
			cut_assert_equal_string(ext_req.c_str(), h->requests[i].c_str());
			cut_assert_equal_string("VALUE\n", h->values[i].c_str());
		}

		// assert
		assert_queue_size(0);
		assert_state(true, false);
	}

	void test_on_post_proxy_write_invalid_destination() {
		// prepare
		cluster::node master = cl->set_node("localhost", port, cluster::role_master, cluster::state_active);
		cl->set_partition(0, master);
		cut_assert_equal_int(0, cl_repl->start("localhost", port, 1, st, cl));
		sleep(6);  // waiting for connection failed

		// execute
		shared_connection c(new connection_sstream(" TEST 0 0 5\r\nVALUE\r\n"));
		mock_op_proxy_write op(c, NULL, NULL);
		cut_assert_equal_int(-1, cl_repl->on_post_proxy_write(&op));
	}
}

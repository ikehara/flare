/**
 *	test_handler_dump_replication.cc
 *
 *	@author Masanori Yoshimoto <masanori.yoshimoto@gree.net>
 */

#include "app.h"
#include "stats.h"
#include "handler_dump_replication.h"
#include "mock_cluster.h"
#include "server.h"
#include "mock_storage.h"

#include <cppcutter.h>

using namespace std;
using namespace gree::flare;

namespace test_handler_dump_replication {
	void sa_usr1_handler(int sig) {
		// just ignore
	}

	int										port;
	mock_cluster*					cl;
	mock_storage*					st;
	server*								s;
	thread_pool*					tp;
	vector<shared_connection_tcp>		cs;
	struct sigaction	prev_sigusr1_action;

	void setup() {
		struct sigaction sa;
		memset(&sa, 0, sizeof(sa));
		sa.sa_handler = sa_usr1_handler;
		if (sigaction(SIGUSR1, &sa, &prev_sigusr1_action) < 0) {
			log_err("sigaction for %d failed, %s (%d)", SIGUSR1, util::strerror(errno), errno);
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
	}

	void teardown() {
		for (int i = 0; i < cs.size(); i++) {
			cs[i]->close();
		}
		cs.clear();
		if (s) {
			s->close();
		}
		tp->shutdown();
		st->close();

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

	void prepare_storage(int item_num, int item_size) {
		for (int i = 0; i < item_num; i++) {
			string key = "key" + boost::lexical_cast<string>(i);
			ostringstream value;
			for (int j = 0; j < item_size; j++) {
				value << "o";
			}
			st->set_helper(key, value.str(), 0);
		}
	}

	shared_thread start_handler(bool listen = true) {
		shared_thread t = tp->get(thread_pool::thread_type_dump_replication);
		handler_dump_replication* h = new handler_dump_replication(t, cl, st, "localhost", port);
		t->trigger(h, true, false);
		if (listen) {
			s->listen(port);
			cs = s->wait();
		}
		return t;
	}

	void response_dump(string response) {
		if (response.length() > 0 && cs.size() > 0) {
			cs[0]->writeline(response.c_str());
		}
	}

	void test_run_success_single_partition() {
		cluster::node master = cl->set_node("localhost", port, cluster::role_master, cluster::state_active);
		cl->set_partition(0, master);
		prepare_storage(3, 5);
		shared_thread t = start_handler();

		for (int i = 0; i < 3; i++) {
			response_dump("STORED");
		}

		sleep(1);  // waiting for dump completed
		cut_assert_equal_boolean(false, t->is_running());
		cut_assert_equal_string("", t->get_state().c_str());
	}

	void test_run_success_two_partition() {
		cluster::node master1 = cl->set_node("localhost", port, cluster::role_master, cluster::state_active);
		cluster::node master2 = cl->set_node("dummy", port + 1, cluster::role_master, cluster::state_active, 1);
		cl->set_partition(0, master1);
		cl->set_partition(1, master2);
		prepare_storage(10, 5);
		shared_thread t = start_handler();

		// assigned key to this partition is half of all keys when hash algorithm is simple
		// (key0, key2, key4, key6, key8)
		for (int i = 0; i < 5; i++) {
			response_dump("STORED");
		}

		sleep(1);
		cut_assert_equal_boolean(false, t->is_running());
		cut_assert_equal_string("", t->get_state().c_str());
	}

	void test_run_success_wait() {
		cluster::node master = cl->set_node("localhost", port, cluster::role_master, cluster::state_active);
		cl->set_partition(0, master);
		cl->set_reconstruction_interval(1.5 * 1000 * 1000); // 1.5 secs
		prepare_storage(3, 5); // 1.5 secs * 3 = 4.5 secs
		shared_thread t = start_handler();

		for (int i = 0; i < 3; i++) {
			response_dump("STORED");
		}

		sleep(4);
		// still under dump replication
		cut_assert_equal_boolean(true, t->is_running());
		cut_assert_equal_string("execute", t->get_state().c_str());
		sleep(1);
		cut_assert_equal_boolean(false, t->is_running());
		cut_assert_equal_string("", t->get_state().c_str());
	}

	void test_run_success_bwlimit() {
		cluster::node master = cl->set_node("localhost", port, cluster::role_master, cluster::state_active);
		cl->set_partition(0, master);
		cl->set_reconstruction_bwlimit(1); // 1 KB
		prepare_storage(4, 1024); // 4 KB / 1 KB = 4 secs (about)
		shared_thread t = start_handler();

		for (int i = 0; i < 10; i++) {
			response_dump("STORED");
		}

		sleep(3.5);
		// still under dump replication
		cut_assert_equal_boolean(true, t->is_running());
		cut_assert_equal_string("execute", t->get_state().c_str());
		sleep(1);
		cut_assert_equal_boolean(false, t->is_running());
		cut_assert_equal_string("", t->get_state().c_str());
	}

	void test_run_success_shutdown_graceful() {
		cluster::node master = cl->set_node("localhost", port, cluster::role_master, cluster::state_active);
		cl->set_partition(0, master);
		prepare_storage(2, 5);
		st->iter_wait = 200 * 1000;  // 200 msecs
		shared_thread t = start_handler();

		response_dump("STORED");  // response only first data
		cut_assert_equal_int(0, t->shutdown(true, false));
		sleep(1);
		cut_assert_equal_boolean(false, t->is_running());
		cut_assert_equal_string("", t->get_state().c_str());
	}

	void test_run_success_shutdown_not_graceful() {
		cluster::node master = cl->set_node("localhost", port, cluster::role_master, cluster::state_active);
		cl->set_partition(0, master);
		prepare_storage(2, 5);
		st->iter_wait = 200 * 1000;  // 200 msecs
		shared_thread t = start_handler();

		response_dump("STORED");  // response only first data
		cut_assert_equal_int(0, t->shutdown(false, false));
		sleep(1);
		cut_assert_equal_boolean(false, t->is_running());
		cut_assert_equal_string("shutdown", t->get_state().c_str());
	}

	void test_run_failure_unreachable_connection() {
		cluster::node master = cl->set_node("localhost", port, cluster::role_master, cluster::state_active);
		cl->set_partition(0, master);
		prepare_storage(3, 5);
		shared_thread t = start_handler(false);

		sleep(6);  // waiting for connection failure
		cut_assert_equal_boolean(false, t->is_running());
		cut_assert_equal_string("", t->get_state().c_str());
	}

	void test_run_failure_database_busy() {
		cluster::node master = cl->set_node("localhost", port, cluster::role_master, cluster::state_active);
		cl->set_partition(0, master);
		prepare_storage(3, 5);
		st->iter_begin();
		shared_thread t = start_handler();

		sleep(1);  // waiting for database busy
		cut_assert_equal_boolean(false, t->is_running());
		cut_assert_equal_string("", t->get_state().c_str());
	}
}
// vim: foldmethod=marker tabstop=2 shiftwidth=2 noexpandtab autoindent

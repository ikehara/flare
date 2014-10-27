/**
 *	test_handler_cluster_replication.cc
 *
 *	@author Masanori Yoshimoto <masanori.yoshimoto@gree.net>
 */

#include "app.h"
#include "stats.h"
#include "handler_cluster_replication.h"
#include "queue_forward_query.h"
#include "server.h"
#include "storage.h"

#include <cppcutter.h>

using namespace std;
using namespace gree::flare;

namespace test_handler_cluster_replication {
	void sa_usr1_handler(int sig) {
		// just ignore
	}

	int							port;
	server*					s;
	thread_pool*		tp;
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

		tp = new thread_pool(1);
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

		delete s;
		delete tp;
		delete stats_object;

		if (sigaction(SIGUSR1, &prev_sigusr1_action, NULL) < 0) {
			log_err("sigaction for %d failed: %s (%d)", SIGUSR1, util::strerror(errno), errno);
			return;
		}
	}

	storage::entry get_entry(string input, storage::parse_type type, string value = "") {
		storage::entry e;
		e.parse(input.c_str(), type);
		if (e.size > 0 && value.length() > 0) {
			shared_byte data(new uint8_t[e.size]);
			memcpy(data.get(), value.c_str(), e.size);
			e.data = data;
		}
		return e;
	}

	shared_thread start_handler(bool listen = true) {
		shared_thread t = tp->get(thread_pool::thread_type_cluster_replication);
		handler_cluster_replication* h = new handler_cluster_replication(t, "localhost", port);
		t->trigger(h, true, false);
		if (listen) {
			s->listen(port);
			cs = s->wait();
		}
		return t;
	}

	void replicate(shared_thread t, shared_thread_queue q, string response = "") {
		q->sync_ref();
		t->enqueue(q);
		if (response.length() > 0 && cs.size() > 0) {
			cs[0]->writeline(response.c_str());
		}
		q->sync();
	}

	void test_run_success() {
		shared_thread t = start_handler();
		storage::entry e = get_entry(" key 0 0 5 3", storage::parse_type_set, "VALUE");
		shared_queue_forward_query q(new queue_forward_query(e, "set"));

		replicate(t, q, "STORED");

		cut_assert_equal_boolean(true, q->is_success());
		cut_assert_equal_int(op::result_stored, q->get_result());
		sleep(1);  // waiting for process queue completed
		cut_assert_equal_boolean(true, t->is_running());
		cut_assert_equal_string("wait", t->get_state().c_str());
	}

	void test_run_shutdown_graceful() {
		shared_thread t = start_handler();

		cut_assert_equal_int(0, t->shutdown(true, false));
		sleep(1);  // waiting for shutdown completed
		cut_assert_equal_boolean(false, t->is_running());
		cut_assert_equal_string("", t->get_state().c_str());
	}

	void test_run_shutdown_not_graceful() {
		shared_thread t = start_handler();

		cut_assert_equal_int(0, t->shutdown(false, false));
		sleep(1);  // waiting for shutdown completed
		cut_assert_equal_boolean(false, t->is_running());
		cut_assert_equal_string("shutdown", t->get_state().c_str());
		cut_assert_equal_boolean(true, t->is_shutdown_request());
	}

	void test_run_failure_unreachable_connection() {
		shared_thread t = start_handler(false);

		sleep(6);  // waiting for connection failure
		cut_assert_equal_boolean(false, t->is_running());
		cut_assert_equal_string("", t->get_state().c_str());
	}
}
// vim: foldmethod=marker tabstop=2 shiftwidth=2 noexpandtab autoindent

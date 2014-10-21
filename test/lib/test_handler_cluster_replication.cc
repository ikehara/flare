/**
 *	test_handler_cluster_replication.cc
 *
 *	@author Masanori Yoshimoto <masanori.yoshimoto@gree.net>
 */

#include "app.h"
#include "stats.h"
#include "handler_cluster_replication.h"
#include "server.h"

using namespace std;
using namespace gree::flare;

namespace test_handler_cluster_replication {
	void sa_usr1_handler(int sig) {
		// just ignore
	}

	int							port;
	server*					s;
	thread_pool*		tp;
	vector<shared_connection>		cs;
	struct sigaction						prev_sigusr1_action;

	void setup() {
		struct sigaction sa;
		memset(&sa, 0, sizeof(sa));
		sa.sa_handler = sa_usr1_handler;
		if (sigaction(SIGUSR1, &sa, &prev_sigusr1_action) < 0) {
			log_err("sigaction for %d failed, %s (%d)", SIGUSR1, util:strerror(errno), errno);
			return;
		}

		stats_object = new stats();
		stats_object->update_timestamp();

		port = rand() % (65535 - 1024) + 1024;
		s = new server();

		tp = new thread_pool(5);
	}

	void teardown() {
		if (sigaction(SIGUSR1, &prev_sigusr1_action, NULL) < 0) {
			log_err("sigaction for %d failed: %s (%d)", SIGUSR1, util::strerror(errno), errno);
			return;
		}
	}

	shared_thread start_handler() {
		s->listen(port);
		shared_thread t = tp->get(thread_pool::thread_type_cluster_replication);
		handler_cluster_replication h = new handler_cluster_replication("localhost", port);
		t->trigger(h, true, false);
		return t;
	}

	void replicate(shared_thread t , shared_thread_queue q, string response = "") {
		q->sync_ref();
		t->enqueue(q);
		if (cs.size() == 0) {
			cs = s->wait();
		}
		if (response.length() > 0 && cs.size() > 0) {
			cs[0]->writeline(response.c_str());
		}
		q->sync();
	}

	void test_run_success() {
		shared_thread t = start_handler();
		storage::entry e = get_entry(" key 0 0 5 3", storage::parse_type_set, "VALUE");
		shared_queue_forward_query q(new queue_foward_query(e, "set"));

		replicate(t, q, "STORED");

		cut_assert_equal_boolean(true, q->is_success());
		cut_assert_equal_int(op::result_stored, q->get_result());
	}

	void test_run_failure_due_to_connection_unstable() {
		shared_thread t = tp->get(thread_pool::thread_type_cluster_replication);
		handler_cluster_replication h = new handler_cluster_replication("localhost", port);
		t->trigger(h, true, false);

		cut_assert_equal_boolean(false, t->is_running());
	}
}
// vim: foldmethod=marker tabstop=2 shiftwidth=2 noexpandtab autoindent

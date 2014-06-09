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
#include "thread_pool.h"

#include "storage_simple_map.h"
#include "connection_iostream.h"

#include <cppcutter.h>


using namespace std;
using namespace gree::flare;

namespace test_cluster_replication {
	struct mock_cluster : public cluster {
		using cluster::_node_key;
		using cluster::_node_map;
		using cluster::_node_partition_map;
		mock_cluster(thread_pool* tp, string server_name, int server_port):
			cluster(tp, server_name, server_port) {
			this->_key_hash_algorithm = storage::hash_algorithm_simple;
			this->_key_resolver = new key_resolver_modular(1024, 1, 4096);
			this->prepare_node(cluster::role_proxy);
		}

		~mock_cluster() {
		}

		void prepare_node(cluster::role r) {
			cluster::node n;
			n.node_server_name = this->_server_name;
			n.node_server_port = this->_server_port;
			n.node_role = r;
			n.node_partition = -1;
			this->_node_map[this->_node_key] = n;

			cluster::partition p;
			cluster::partition_node pn;
			pn.node_key = this->_node_key;
			pn.node_balance = 0;

			if (r == cluster::role_master) {
				p.master = pn;
				this->_node_partition_map[n.node_partition] = p;
			} else if (r == cluster::role_slave) {
				p.slave.push_back(pn);
				this->_node_partition_map[n.node_partition] = p;
			}
		}
	};

	int port;

	void setup() {
		port = rand() % (65535 - 1024) + 1024;
		stats_object = new stats();
		stats_object->update_timestamp();
	}

	void teardown() {
		delete stats_object;
	}

	server* create_server(int port) {
		server* s = new server();
		s->listen(port);
		return s;
	}

	void check(cluster_replication& cl_repl, thread_pool& tp, bool started, string server_name, int server_port, int concurrency, bool dump, bool sync = false) {
		cut_assert_equal_boolean(started, cl_repl.is_started());
		cut_assert_equal_string(server_name.c_str(), cl_repl.get_server_name().c_str());
		cut_assert_equal_int(server_port, cl_repl.get_server_port());
		cut_assert_equal_int(concurrency, cl_repl.get_concurrency());
		thread_pool::local_map m_cl = tp.get_active(thread_pool::thread_type_cluster_replication);
		if (started) {
			cut_assert_equal_int(concurrency, m_cl.size());
		} else {
			cut_assert_equal_int(0, m_cl.size());
		}
		thread_pool::local_map m_dump = tp.get_active(thread_pool::thread_type_dump_replication);
		if (dump) {
			cut_assert_equal_int(1, m_dump.size());
		} else {
			cut_assert_equal_int(0, m_dump.size());
		}
		cut_assert_equal_boolean(sync, cl_repl.get_sync());
	}

	void test_default_value() {
		thread_pool tp(1);
		cluster_replication cl_repl(&tp);
		check(cl_repl, tp, false, "", 0, 0, false);
	}

	void test_set_sync() {
		thread_pool tp(1);
		cluster_replication cl_repl(&tp);
		cut_assert_equal_boolean(false, cl_repl.get_sync());
		cut_assert_equal_int(0, cl_repl.set_sync(true));
		cut_assert_equal_boolean(true, cl_repl.get_sync());
		cut_assert_equal_int(0, cl_repl.set_sync(false));
		cut_assert_equal_boolean(false, cl_repl.get_sync());
	}

	void test_start_when_proxy() {
		// prepare
		server* s = create_server(port);
		thread_pool tp(1);
		mock_cluster cl(NULL, "localhost", port);
		cl.prepare_node(cluster::role_proxy);
		storage_simple_map st("", 0, 0);

		// execute
		cluster_replication cl_repl(&tp);
		cut_assert_equal_int(0, cl_repl.start("localhost", port, 1, &st, &cl));
		sleep(1);  // waiting for connection establishment

		// assert
		check(cl_repl, tp, true, "localhost", port, 1, false);
		delete s;
	}

	void test_start_when_master() {
		// prepare
		server* s = create_server(port);
		thread_pool tp(2);
		mock_cluster cl(NULL, "localhost", port);
		cl.prepare_node(cluster::role_master);
		storage_simple_map st("", 0, 0);
		st._wait_iter = 2 * 1000 * 1000; // 2 s

		// execute
		cluster_replication cl_repl(&tp);
		cut_assert_equal_int(0, cl_repl.start("localhost", port, 1, &st, &cl));
		sleep(1);  // waiting for connection establishment

		// assert
		check(cl_repl, tp, true, "localhost", port, 1, true);
		sleep(2);  // waiting for dump replication completed
		check(cl_repl, tp, true, "localhost", port, 1, false);
		delete s;
	}

	void test_start_when_slave() {
		// prepare
		server* s = create_server(port);
		thread_pool tp(2);
		mock_cluster cl(NULL, "localhost", port);
		cl.prepare_node(cluster::role_slave);
		storage_simple_map st("", 0, 0);
		st._wait_iter = 2 * 1000 * 1000; // 2s

		// execute
		cluster_replication cl_repl(&tp);
		cut_assert_equal_int(0, cl_repl.start("localhost", port, 1, &st, &cl));
		sleep(1);  // waiting for connection establishment

		// asert
		check(cl_repl, tp, true, "localhost", port, 1, true);
		sleep(2);  // waiting for dump replication completed
		check(cl_repl, tp, true, "localhost", port, 1, false);
		delete s;
	}

	void test_start_with_multiple_concurrency() {
		// prepare
		server* s = create_server(port);
		thread_pool tp(4);
		mock_cluster cl(NULL, "localhost", port);
		storage_simple_map st("", 0, 0);

		// execute
		cluster_replication cl_repl(&tp);
		cut_assert_equal_int(0, cl_repl.start("localhost", port, 3, &st, &cl));
		sleep(1);  // waiting for connection establishment

		// assert
		check(cl_repl, tp, true, "localhost", port, 3, false);
		delete s;
	}

	void test_start_with_no_concurrency() {
		// prepare
		thread_pool tp(1);
		mock_cluster cl(NULL, "localhost", port);
		storage_simple_map st("", 0, 0);

		// execute
		cluster_replication cl_repl(&tp);
		cut_assert_equal_int(-1, cl_repl.start("localhost", port, 0, &st, &cl));

		// assert
		check(cl_repl, tp, false, "", 0, 0, false);
	}

	void test_start_in_started_state() {
		// prepare
		server* s = create_server(port);
		thread_pool tp(1);
		mock_cluster cl(NULL, "localhost", port);
		storage_simple_map st("", 0, 0);

		// execute
		cluster_replication cl_repl(&tp);
		cut_assert_equal_int(0, cl_repl.start("localhost", port, 1, &st, &cl));
		sleep(1);  // waiting for connection establishment

		// assert
		check(cl_repl, tp, true, "localhost", port, 1, false);
		cut_assert_equal_int(-1, cl_repl.start("localhost", port, 1, &st, &cl));
		delete s;
	}

	void test_stop_in_not_started_state() {
		// prepare
		thread_pool tp(1);

		// execute
		cluster_replication cl_repl(&tp);
		cut_assert_equal_int(0, cl_repl.stop());

		// assert
		check(cl_repl, tp, false, "", 0, 0, false);
	}

	/* unable to test cluster_replication#stop because test is interrupted by sigusr1 caused by shutting down threads
	void test_stop_in_stated_state() {
		thread_pool tp(2);
		cluster_replication cl_repl(&tp);
		cl.prepare_node(cluster::role_master);
		cut_assert_equal_int(0, cl_repl.start("localhost", port, 1, &st, &cl));
		check(cl_repl, tp, true, "localhost", port, 1, true);
		cut_assert_equal_int(0, cl_repl.stop());
		sleep(1); // cluster replication shutdown threads asynchronously.
		check(cl_repl, tp, false, "localhost", port, 1, false);
	}

	void test_stop_with_multiple_concurrency() {
		thread_pool tp(4);
		cluster_replication cl_repl(&tp);
		cl.prepare_node(cluster::role_master);
		cut_assert_equal_int(0, cl_repl.start("localhost", port, 3, &st, &cl));
		check(cl_repl, tp, true, "localhost", port, 3, true);
		cut_assert_equal_int(0, cl_repl.stop());
		sleep(1); // cluster replication shutdown threads asynchronously.
		check(cl_repl, tp, false, "localhost", port, 3, false);
	}
	*/

	void test_on_pre_proxy_read() {
		// prepare
		thread_pool tp(1);
		mock_cluster cl(NULL, "localhost", port);
		storage_simple_map st("", 0, 0);

		// execute
		cluster_replication cl_repl(&tp);
		shared_connection c(new connection_sstream(" TEST"));
		op_get op(c, &cl, &st);
		cut_assert_equal_int(0, cl_repl.on_pre_proxy_read(&op));
	}

	void test_on_pre_proxy_write() {
		// prepare
		thread_pool tp(1);
		mock_cluster cl(NULL, "localhost", port);
		storage_simple_map st("", 0, 0);

		// execute
		cluster_replication cl_repl(&tp);
		shared_connection c(new connection_sstream(" TEST 0 0 5\r\nVALUE\r\n"));
		op_set op(c, &cl, &st);
		cut_assert_equal_int(0, cl_repl.on_pre_proxy_write(&op));
	}

	void test_on_post_proxy_write_in_not_started_state() {
		// prepare
		thread_pool tp(1);
		mock_cluster cl(NULL, "localhost", port);
		storage_simple_map st("", 0, 0);

		// execute
		cluster_replication cl_repl(&tp);
		shared_connection c(new connection_sstream(" TEST 0 0 5\r\nVALUE\r\n"));
		op_set op(c, &cl, &st);
		cut_assert_equal_int(-1, cl_repl.on_post_proxy_write(&op));
	}

	void test_on_post_proxy_write_with_single_concurrency_async() {
		// prepare
		server* s = create_server(port);
		thread_pool tp(1);
		mock_cluster cl(NULL, "localhost", port);
		storage_simple_map st("", 0, 0);
		cluster_replication cl_repl(&tp);
		cut_assert_equal_int(0, cl_repl.start("localhost", port, 1, &st, &cl));
		sleep(1);  // waiting for connection establishment
		check(cl_repl, tp, true, "localhost", port, 1, false);

		// execute
		shared_connection c(new connection_sstream(" TEST 0 0 5\r\nVALUE\r\n"));
		op_set op(c, &cl, &st);
		cut_assert_equal_int(0, cl_repl.on_post_proxy_write(&op));
		sleep(1);
		vector<shared_connection_tcp> cs = s->wait();
		cs[0]->writeline("STORED");

		// assert
		check(cl_repl, tp, true, "localhost", port, 1, false);
		delete s;
	}

	void test_on_post_proxy_write_with_single_concurrency_sync() {
		// prepare
		server* s = create_server(port);
		thread_pool tp(1);
		mock_cluster cl(NULL, "localhost", port);
		storage_simple_map st("", 0, 0);
		cluster_replication cl_repl(&tp);
		cl_repl.set_sync(true);
		cut_assert_equal_int(0, cl_repl.start("localhost", port, 1, &st, &cl));
		sleep(1);  // waiting for connection establishment
		check(cl_repl, tp, true, "localhost", port, 1, false, true);

		// execute
		shared_connection c(new connection_sstream(" TEST 0 0 5\r\nVALUE\r\n"));
		op_set op(c, &cl, &st);
		cut_assert_equal_int(0, cl_repl.on_post_proxy_write(&op));
		sleep(1);
		vector<shared_connection_tcp> cs = s->wait();
		cs[0]->writeline("STORED");

		// assert
		check(cl_repl, tp, true, "localhost", port, 1, false, true);
		delete s;
	}

	void test_on_post_proxy_write_with_multiple_concurrency_async() {
		// prepare
		server* s = create_server(port);
		thread_pool tp(3);
		mock_cluster cl(NULL, "localhost", port);
		storage_simple_map st("", 0, 0);
		cluster_replication cl_repl(&tp);
		cut_assert_equal_int(0, cl_repl.start("localhost", port, 3, &st, &cl));
		sleep(1);  // waiting for connection establishment
		check(cl_repl, tp, true, "localhost", port, 3, false);

		// execute
		for (int i = 0; i < 3; i++) {
			shared_connection c(new connection_sstream(" TEST 0 0 5\r\nVALUE\r\n"));
			op_set op(c, &cl, &st);
			cut_assert_equal_int(0, cl_repl.on_post_proxy_write(&op));
			sleep(1);
			vector<shared_connection_tcp> cs = s->wait();
			cs[0]->writeline("STORED");
		}

		// assert
		check(cl_repl, tp, true, "localhost", port, 3, false);
		sleep(3);
		delete s;
	}

	void test_on_post_proxy_write_with_multiple_concurrency_sync() {
		// prepare
		server* s = create_server(port);
		thread_pool tp(3);
		mock_cluster cl(NULL, "localhost", port);
		storage_simple_map st("", 0, 0);
		cluster_replication cl_repl(&tp);
		cl_repl.set_sync(true);
		cut_assert_equal_int(0, cl_repl.start("localhost", port, 3, &st, &cl));
		sleep(1);  // waiting for connection establishment
		check(cl_repl, tp, true, "localhost", port, 3, false, true);

		// execute
		for (int i = 0; i < 3; i++) {
			shared_connection c(new connection_sstream(" TEST 0 0 5\r\nVALUE\r\n"));
			op_set op(c, &cl, &st);
			cut_assert_equal_int(0, cl_repl.on_post_proxy_write(&op));
			sleep(1);
			vector<shared_connection_tcp> cs = s->wait();
			cs[0]->writeline("STORED");
		}

		// assert
		check(cl_repl, tp, true, "localhost", port, 3, false, true);
		delete s;
	}

	void test_on_post_proxy_write_invalid_destination() {
		// prepare
		thread_pool tp(1);
		mock_cluster cl(NULL, "localhost", port);
		storage_simple_map st("", 0, 0);
		cluster_replication cl_repl(&tp);
		cut_assert_equal_int(0, cl_repl.start("localhost", port, 1, &st, &cl));
		sleep(5);  // waiting for connection failed

		// execute
		shared_connection c(new connection_sstream(" TEST 0 0 5\r\nVALUE\r\n"));
		op_set op(c, &cl, &st);
		cut_assert_equal_int(-1, cl_repl.on_post_proxy_write(&op));
	}
}

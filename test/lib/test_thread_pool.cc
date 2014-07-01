/**
 *	test_thread_pool.cc
 *
 *	@author	Benjamin Surma <benjamin.surma@gree.net>
 */

#include <cppcutter.h>

#include <app.h>
#include <thread_pool.h>
#include <pthread.h>

using namespace gree::flare;

namespace test_thread_pool
{
	void sa_usr1_handler(int sig) {
		// just ignore
	}

	class thread_handler_test : public thread_handler {
	public:
		thread_handler_test(shared_thread t): thread_handler(t) {};
		int run () { sleep(2); return 0; };
	};

	struct sigaction prev_sigusr1_action;

	void setup() {
		stats_object = new stats();
		stats_object->update_timestamp();
		
		struct sigaction sa;
		// SIGUSR1
		memset(&sa, 0, sizeof(sa));
		sa.sa_handler = sa_usr1_handler;
		if (sigaction(SIGUSR1, &sa, &prev_sigusr1_action) < 0) {
			// TODO: show a error message or throw a error here.
			return;
		}
	}

	struct thread_pool_test : public thread_pool
	{
		thread_pool_test():
			thread_pool(128) { }
		~thread_pool_test() { }

		using thread_pool::_index;
		using thread_pool::_global_map;
		using thread_pool::_pool;
	};

	void test_get_thread() {
		// Preparation
		thread_pool_test pool;
		shared_thread t = pool.get(1);
		thread_handler_test *th = new thread_handler_test(t);
		t->trigger(th);
		sleep(1);
		t->shutdown();
	}

	void test_duplicate_thread_id_different_types() {
		// Preparation
		thread_pool_test pool;
		cut_assert_equal_int(1, pool._index.fetch());
		//	Put a dummy thread (Type 0, ID 1, ensuring collision) in the global map
		shared_thread dummy(new thread(&pool));
		dummy->setup(0, pool._index.fetch());
		pool._global_map[0][dummy->get_id()] = dummy;
		// Test
		// 	Fetch thread, ID should be != 1
		shared_thread thread = pool.get(1);
		::sleep(1); // Avoids memory corruption when pool is destroyed too early
		cut_assert_not_equal_int(dummy->get_id(), thread->get_id());
	}

	void test_duplicate_thread_id_same_type() {
		// Preparation
		thread_pool_test pool;
		cut_assert_equal_int(1, pool._index.fetch());
		//	Put a dummy thread (Type 0, ID 1, ensuring collision) in the global map
		shared_thread dummy(new thread(&pool));
		dummy->setup(0, pool._index.fetch());
		pool._global_map[0][dummy->get_id()] = dummy;
		// Test
		// 	Fetch thread, ID should be != 1
		shared_thread thread = pool.get(0);
		::sleep(1); // Avoids memory corruption when pool is destroyed too early
		cut_assert_not_equal_int(dummy->get_id(), thread->get_id());
	}

	void teardown()
	{
		// SIGUSR1
		if (sigaction(SIGUSR1, &prev_sigusr1_action, NULL) < 0) {
			// TODO: show a error message or throw a error here.
			return;
		}
		delete stats_object;
	}
}

// vim: foldmethod=marker tabstop=2 shiftwidth=2 noexpandtab autoindent

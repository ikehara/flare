/**
 *	handler_monitor.cc
 *
 *	implementation of gree::flare::handler_controller
 *
 *	@author	Kiyoshi Ikehara <kiyoshi.ikehara@gree.net>
 *
 *	$Id$
 */
#include "handler_controller.h"
#include "queue_node_state.h"

namespace gree {
namespace flare {

// {{{ global functions
// }}}

// {{{ ctor/dtor
/**
 *	ctor for handler_monitor
 */
handler_controller::handler_controller(shared_thread t, cluster* cl):
		thread_handler(t),
		_cluster(cl) {
}

/**
 *	dtor for handler_controller
 */
handler_controller::~handler_controller() {
}
// }}}

// {{{ operator overloads
// }}}

// {{{ public methods
int handler_controller::run() {
	for (;;) {
		this->_thread->set_state("wait");
		this->_thread->set_op("");

		if (this->_thread->is_shutdown_request()) {
			log_info("thread shutdown request -> breaking loop", 0); 
			this->_thread->set_state("shutdown");
			break;
		}

		// dequeue
		shared_thread_queue q;
		if (this->_thread->dequeue(q, 0) < 0) {
			continue;
		}
		if (this->_thread->is_shutdown_request()) {
			log_info("thread shutdown request -> breaking loop", 0); 
			this->_thread->set_state("shutdown");
			break;
		}

		this->_process_queue(q);
		q->sync_unref();
	}

	return 0;
}
// }}}

// {{{ protected methods
int handler_controller::_process_queue(shared_thread_queue q) {
	log_debug("queue: %s", q->get_ident().c_str());
	this->_thread->set_state("execute");
	this->_thread->set_op(q->get_ident());

	if (q->get_ident() == "node_state") {
		shared_queue_node_state r = shared_dynamic_cast<queue_node_state, thread_queue>(q);
		cluster::node n = this->_cluster->get_node(r->get_node_server_name(), r->get_node_server_port());
		if (r->get_operation() == queue_node_state::state_operation_down) {
			if (n.node_state != cluster::state_down) {
				this->_cluster->down_node(r->get_node_server_name(), r->get_node_server_port());
			} else {
				log_warning("no need to down %s:%d -> skip processing",
										r->get_node_server_name().c_str(), r->get_node_server_port(), q->get_ident().c_str());
			}
		} else if (r->get_operation() == queue_node_state::state_operation_up) {
			if (n.node_state == cluster::state_down) {
				this->_cluster->up_node(r->get_node_server_name(), r->get_node_server_port());
			} else {
				log_warning("no need to up %s:%d -> skip processing",
										r->get_node_server_name().c_str(), r->get_node_server_port(), q->get_ident().c_str());
			}
		}
	} else {
		log_warning("unknown queue [ident=%s] -> skip processing", q->get_ident().c_str());
		return -1;
	}

	return 0;
}
// }}}

// {{{ private methods
// }}}

}	// namespace flare
}	// namespace gree

// vim: foldmethod=marker tabstop=2 shiftwidth=2 autoindent

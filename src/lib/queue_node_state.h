/**
 *	queue_node_state.h
 *
 *	@author	Kiyoshi Ikehara <kiyoshi.ikehara@gree.net>
 *
 *	$Id$
 */
#ifndef	__QUEUE_NODE_STATE_H__
#define	__QUEUE_NODE_STATE_H__

#include "cluster.h"
#include "thread_queue.h"

using namespace std;
using namespace boost;

namespace gree {
namespace flare {

typedef class queue_node_state queue_node_state;
typedef shared_ptr<queue_node_state> shared_queue_node_state;

/**
 *	node sync queue class
 */
class queue_node_state : public thread_queue {
public:
	enum							state_operation {
		state_operation_up,
		state_operation_down,
	};
protected:
	string									_node_server_name;
	int											_node_server_port;
	state_operation					_operation;
public:
	queue_node_state(string node_server_name, int node_server_port, state_operation operation);
	virtual ~queue_node_state();

public:
	string get_node_server_name() { return this->_node_server_name; }
	int get_node_server_port() { return this->_node_server_port; }
	state_operation get_operation() { return this->_operation; }
};

}	// namespace flare
}	// namespace gree

#endif	// __QUEUE_NODE_STATE_H__
// vim: foldmethod=marker tabstop=2 shiftwidth=2 autoindent

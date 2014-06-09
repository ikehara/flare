/**
 *	handler_cluster_replication.h
 *
 *	@author	Masanori Yoshimoto <masanori.yoshimoto@gree.net>
 *
 *	$Id$
 */
#ifndef	HANDLER_CLUSTER_REPLICATION_H
#define	HANDLER_CLUSTER_REPLICATION_H

#include "connection.h"
#include "cluster.h"
#include "thread_handler.h"
#include "thread_queue.h"

using namespace std;
using namespace boost;

namespace gree {
namespace flare {

/**
 *	inter-cluster replication thread handler class
 */
class handler_cluster_replication : public thread_handler {
protected:
	string							_replication_server_name;
	int									_replication_server_port;
	shared_connection		_connection;

public:
	handler_cluster_replication(shared_thread t, string server_name, int server_port);
	virtual ~handler_cluster_replication();

	virtual int run();

protected:
	int _process_queue(shared_thread_queue q);
};

}	// namespace flare
}	// namespace gree

#endif	// HANDLER_CLUSTER_REPLICATION_H
// vim: foldmethod=marker tabstop=2 shiftwidth=2 autoindent
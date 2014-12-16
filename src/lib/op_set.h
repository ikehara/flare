/*
 * Flare
 * --------------
 * Copyright (C) 2008-2014 GREE, Inc.
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
/**
 *	op_set.h
 *
 *	@author	Masaki Fujimoto <fujimoto@php.net>
 *
 *	$Id$
 */
#ifndef	OP_SET_H
#define	OP_SET_H

#include "op_proxy_write.h"

using namespace std;

namespace gree {
namespace flare {

/**
 *	opcode class (set)
 */
class op_set : public op_proxy_write {
protected:
	int			_behavior;

public:
	op_set(shared_connection c, cluster* cl, storage* st);
	virtual ~op_set();

protected:
	op_set(shared_connection c, string ident, binary_header::opcode opcode, cluster* cl, storage* st);
	virtual int _parse_text_server_parameters();
	virtual int _parse_binary_request(const binary_request_header&, const char* body);
	virtual int _run_server();
	virtual int _run_client(storage::entry& e);
	virtual int _parse_text_client_parameters(storage::entry& e);
	
private:
	static const uint8_t _binary_request_required_extras_length = 8;
};

}	// namespace flare
}	// namespace gree

#endif	// OP_SET_H
// vim: foldmethod=marker tabstop=2 shiftwidth=2 autoindent

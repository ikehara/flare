/**
 *	mock_storage.cc
 *
 *	implementation of gree::flare::mock_storage class
 *
 *	@author	Masanori Yoshimoto <masanori.yoshimoto@gree.net>
 *
 *	$Id$
 */
#include "mock_storage.h"

namespace gree {
namespace flare {

// {{{ global functions
// }}}

// {{{ ctor/dtor
mock_storage::mock_storage(string data_dir, int mutex_slot_size, int header_cache_size):
	storage(data_dir, mutex_slot_size, header_cache_size),
	iter_wait(-1),
	_is_iter(false) {
	this->_it = this->_map.end();
}

mock_storage::~mock_storage() {
	this->_map.clear();
}
// }}}

// {{{ operator overloads
// }}}

// {{{ public methods
int mock_storage::set(entry& e, result& r, int b) {
	this->_map.insert(pair<string, storage::entry>(e.key, e));
	r = result_stored;
	return 0;
}

int mock_storage::incr(entry& e, uint64_t value, result& r, bool increment, int b) {
	r = result_not_stored;
	return 0;
}

int mock_storage::get(entry& e, result& r, int b) {
	map<string, storage::entry>::iterator it = this->_map.find(e.key);
	if (it != this->_map.end()) {
		r = result_found;
		e = (*it).second;
	} else {
		r = result_not_found;
	}
	return 0;
}

int mock_storage::remove(entry& e, result& r, int b) {
	this->_map.erase(e.key);
	return 0;
}

int mock_storage::truncate(int b) {
	this->_map.clear();
	return 0;
}

int mock_storage::iter_begin() {
	if (!this->_is_iter) {
		this->_is_iter = true;
		this->_it = this->_map.begin();
		return 0;
	} else {
		return -1;
	}
}

storage::iteration mock_storage::iter_next(string& key) {
	if (!this->_is_iter) {
		return iteration_error;
	}
	if (this->iter_wait > 0) {
		usleep(iter_wait);
	}
	if (this->_it != this->_map.end()) {
		key = (*this->_it).first;
		++this->_it;
		return iteration_continue;
	}
	return iteration_end;
}

int mock_storage::iter_end() {
	if (this->_is_iter) {
		this->_is_iter = false;
		this->_it = this->_map.end();
		return 0;
	} else {
		return -1;
	}
}

void mock_storage::set_helper(const string& key, const string& value, int flag, int version) {
	storage::result result;
	storage::entry entry;
	entry.key = key;
	entry.flag = flag;
	entry.version = version;
	entry.size = value.size();
	entry.data = shared_byte(new uint8_t[entry.size]);
	memcpy(entry.data.get(), value.data(), entry.size);
	this->set(entry, result);
}
// }}}


// {{{ protected methods
// }}}

// {{{ private methods
// }}}

}	// namespace flare
}	// namespace gree

// vim: foldmethod=marker tabstop=2 shiftwidth=2 noexpandtab autoindent

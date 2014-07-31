local util = require "lkfk.util";
local setmetatable = setmetatable;

local _M = util.new_tab(0, 15);

local mt = {__index = _M};

function _M.new(key)
	local head = {};
	head[key] = {
		next = head,
		prev = head
	}
	local l = {
		size = 0;
		key = key;
		head = head;
	};
	return setmetatable(l, mt);
end

function _M.LIST_ENTRY()
	return {
		next = nil, prev = nil
	}
end

function _M.empty(self)
	return self.size == 0;
end

function _M.first(self)
	return self.head[self.key].next;
end

function _M.init(self)
	local key = self.key;
	self.head[key].next = self.head;
	self.head[key].prev = self.head;
	self.size = 0;
end

function _M.next(self, entry)
	return entry[self.key].next;
end

function _M.concat(self, src)
	if src.size == 0 then
		return;
	end
	
	local key = self.key;
	local head1, head2 = self.head[key], src.head[key];
	
	head1.prev[key].next = head2.next;
	head2.prev[key].next = self.head;
	
	head2.next[key].prev = head1.prev;
	head1.prev = head2.prev;
	
	self.size = self.size + src.size;
	
	_M.init(src);
end

function _M.concat_head(self, src)
	if src.size == 0 then
		return;
	end
	
	local key = self.key;
	local head1, head2 = self.head[key], src.head[key];
	
	head1.next[key].prev = head2.prev;
	head2.next[key].prev = self.head;
	
	head2.prev[key].next = head1.next;
	head1.next = head2.next;

	self.size = self.size + src.size;
	_M.init(src);
end

function _M.remove(self, entry)
	local key = self.key;
	entry[key].prev[key].next = entry[key].next;
	entry[key].next[key].prev = entry[key].prev;
	self.size = self.size - 1;
end

function _M.insert_head(self, entry)
	local key = self.key;
	local head = self.head[key];
	
	entry[key].next = head.next;
	head.next[key].prev = entry;
	
	entry[key].prev = self.head;
	head.next = entry;
	self.size = self.size + 1;
end

function _M.insert_tail(self, entry)
	local key = self.key;
	local head = self.head[key];
	
	entry[key].prev = head.prev;
	head.prev[key].next = entry;
	
	entry[key].next = self.head;
	head.prev = entry;
	self.size = self.size + 1;
end

function _M.insert_after(self, prev, entry)
	local key = self.key;
	entry[key].next = prev[key].next;
	prev[key].next[key].prev = entry;
	
	entry[key].prev = prev;
	prev[key].next = entry;
	self.size = self.size + 1;
end

return _M;

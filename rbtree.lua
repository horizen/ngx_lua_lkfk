local util = require "util";
local setmetatable = setmetatable;
--[[
	definition of `tree node`
	node:
	{
		key = 			-- the key for node compare
		left = 			-- left node reference
		right = 		-- right node reference
		parent =		-- parent node reference
		color =  		-- node color
		data = 			-- the data of this node
	}
--]]

local _M = util.new_tab(4, 0);

_M._VERSION = "1.0";

local _BLACK  = 1;
local _RED = 0;
local sentinel = {
	color = _BLACK
};

local function _insert_node(tmp, node)
	local p, tag;
	
	while tmp ~= sentinel do
		p = tmp;
		if node.key - tmp.key < 0  then
			tag = "left";
			tmp = tmp.left;
		else
			tag = "right";
			tmp = tmp.right;
		end
	end
	
	node.color = _RED;
	node.left = sentinel;
	node.right = sentinel;
	p[tag] = node;
	p[tag] = node;
end

local function _rbtree_left_rotate(self, node)
	local tmp = node.right;
	node.right = tmp.left;
	
	if tmp.left ~= sentinel then
		tmp.left.parent = node;
	end
	
	local parent = node.parent;
	tmp.parent = parent;
	
	if node == self.root then
		self.root = tmp;
	elseif node == parent.left then
		parent.left = tmp;
	else
		parent.right = tmp;
	end
	tmp.left = node;
	node.parent = tmp;
end

local function _rbtree_right_rotate(self, node)
	local tmp = node.left;
	node.left = tmp.right;
	
	if tmp.right ~= sentinel then
		tmp.right.parent = node;
	end
	
	local parent = node.parent;
	tmp.parent = parent;
	
	if node == self.root then
		self.root = tmp;
	elseif node == parent.right then
		parent.right = tmp;
	else
		parent.right = tmp;
	end
	tmp.right = node;
	node.parent = tmp;
end

local function _rbtree_min(tmp)
	while tmp.left ~= sentinel do
		tmp = tmp.left;
	end
	return tmp;
end

local mt = {__index = _M};

--[[
	public api
--]]


function _M.new(cmp)
	local rbtree = {
		root = nil,
		cnt = 0
	}
	setmetatable(rbtree, mt);
end


function _M.rbtree_insert(self, node)
	self.cnt = self.cnt + 1;
	if not self.root then
		node.color = _BLACK;
		node.left = sentinel;
		node.right = sentinel;
		node.parent = nil;
		self.root = node;
		return;
	end
	
	_insert_node(self.root, node);
	
	while node ~= self.root and node.parent.color == _RED do
		local parent = node.parent;
		local grandp = parent.parent;
		local uncle;
		if parent == grandp.left then
			uncle = grandp.right;
			if uncle.color == _RED then
				parent.color = _BLACK;
				uncle.color = _BLACK;
				grandp.color = _RED;
				node = grandp;
			else
				if node == parent.right then
					node = parent;
					_rbtree_left_rotate(self, node);
				end
				
				node.parent.color = _BLACK;
				node.parent.parent.color = _RED;
				_rbtree_right_rotate(self, node.parent.parent);
			end
		else
			uncle = grandp.left;
			if uncle.color == _RED then
				parent.color = _BLACK;
				uncle.color = _BLACK;
				grandp.color = _RED;
				node = grandp;
			else
				if node == parent.left then
					node = parent;
					_rbtree_right_rotate(self, node);
				end
				
				node.parent.color = _BLACK;
				node.parent.parent.color = _RED;
				_rbtree_left_rotate(self, node.parent.parent);
			end
		end
	end
	
	self.root.color = _BLACK;
end



function _M.rbtree_delete(self, node)
	local subst, temp;
	self.cnt = self.cnt - 1;
	
	if node.left == sentinel then
		temp = node.right;
		subst = node;
	elseif node.right == sentinel then
		temp = node.left;
		subst = node;
	else
		subst = _rbtree_min(node.right);
		if subst.left == sentinel then
			temp = node.right;
		else
			temp = node.left;
		end
	end
	
	if subst == self.root then
		temp.color = _BLACK;
		self.root = temp;
		return;
	end
	
	if subst ~= node then
		node.key = subst.key;
		node.data = subst.data;
	end
	
	if subst == subst.parent.left then
		subst.parent.left = temp;
	else
		subst.parent.right = temp;
	end
	
	temp.parent = subst.parent;
	
	if subst.color == _RED then
		return;
	end
	
	while temp ~= self.root and temp.color == _BLACK do
		local parent = temp.parent;
		local brother;
		if temp == parent.left then
			brother = parent.right;
			
			if brother.color == _RED then
				parent.color = _RED;
				brother.color = _BLACK;
				_rbtree_left_rotate(self, parent);
				brother = parent.right;
			end
			
			if brother.left.color == _BLACK and brother.right.color == _BLACK then
				brother.color = _RED;
				temp = parent;
			else
				if brother.right.color == _BLACK then
					brother.left.color = _BLACK;
					brother.color = _RED;
					_rbtree_right_rotate(self, brother);
					brother = parent.right;
				end
				
				brother.color = parent.color;
				parent.color = _BLACK;
				brother.right.color = _BLACK;
				_rbtree_left_rotate(self, parent);
				temp = self.root;
			end
		else
			brother = parent.left;
			
			if brother.color == _RED then
				parent.color = _RED;
				brother.color = _BLACK;
				_rbtree_right_rotate(self, parent);
				brother = parent.left;
			end
			
			if brother.left.color == _BLACK and brother.right.color == _BLACK then
				brother.color = _RED;
				temp = parent;
			else
				if brother.left.color == _BLACK then
					brother.right.color = _BLACK;
					brother.color = _RED;
					_rbtree_left_rotate(self, brother);
					brother = parent.left;
				end
				
				brother.color = parent.color;
				parent.color = _BLACK;
				brother.left.color = _BLACK;
				_rbtree_right_rotate(self, parent);
				temp = self.root;
			end
			
			
		end
	end
	
	temp.color = _BLACK;
end	

return _M;
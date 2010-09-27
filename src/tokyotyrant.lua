-- http://github.com/daurnimator/lua-tokyotyrant/commit/3841dd55246b9f459e1b6d38aaa1db785bb1cb01
-- Tokyo Tyrant interface for Lua 5.1
-- Phoenix Sol -- phoenix@burninglabs.com
-- Daurnimator
-- ( mostly translated from Mikio's Ruby interface ) --
-- Thanks, Mikio.

--[[
Copyright 2009 Phoenix Sol (aka Corey Michael Trampe)

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.
]]--

local assert , error , getmetatable , ipairs , pairs , rawget , rawset , setmetatable , tonumber , tostring , type = assert , error , getmetatable , ipairs , pairs , rawget , rawset , setmetatable , tonumber , tostring , type
local tblconcat = table.concat
local floor , fmod = math.floor , math.fmod

local struct = require 'struct'
local socket = require "socket"
pcall ( require , "socket.unix" )

local function module(name) end --trick luadoc
module 'tokyotyrant'

local classmt = { __call = function ( t , ... ) return t.new ( ... ) end }

local constants = {
  MAGIC = 0xC8 ; --a little can go a long way

        MONOULOG = 1 ; --ommit update log (misc function)
  XOLCKREC = 1 ; --record locking (lua extension)
  XOLCKGLB = 2 ; --global locking (lua extension)
  ROCHKCON = 1 ; -- consistency checking (restoration)

  PUT = 0x10 ;
  PUTKEEP = 0x11 ;
  PUTCAT = 0x12 ;
  PUTSHL = 0x13 ;
  PUTNR = 0x18 ;
  OUT = 0x20 ;
  GET = 0x30 ;
  MGET = 0x31 ;
  VSIZ = 0x38 ;
  ITERINIT = 0x50 ;
  ITERNEXT = 0x51 ;
  FWMKEYS = 0x58 ;
  ADDINT = 0x60 ;
  ADDDOUBLE = 0x61 ;
  EXT = 0x68 ;
  SYNC = 0x70 ;
  OPTIMIZE = 0x71 ;
  VANISH = 0x72 ;
  COPY = 0x73 ;
  RESTORE = 0x74 ;
  SETMST = 0x78 ;
  RNUM = 0x80 ;
  SIZE = 0x81 ;
  STAT = 0x88 ;
  MISC = 0x90 ;
}

local recvuchar = function ( sock )
  local r , err = sock:receive ( 1 )
  if not r then return false , err end
  return struct.unpack ( '>B' , r )
end

local recvint32 = function ( sock )
  local r , err = sock:receive ( 4 )
  if not r then return false , err end
  return struct.unpack ( '>i4' , r )
end

local recvint64 = function ( sock )
  local r , err = sock:receive ( 8 )
  if not r then return false , err end
  return struct.unpack ( '>i8' , r )
end

local rdb = setmetatable  ( { } , classmt )
local mt = {
  __index = function ( t , k )
    local v = rawget ( rdb , k )
    if v ~= nil then return v end
    return assert ( t:get ( k ) )
  end ;
  __newindex = function ( t , k , v )
    assert ( t:put ( k , v ) )
  end ;
  type = "RDBO" ;
}

function rdb:isRDBO ( )
  local t = ( getmetatable ( self ) or { } ).type
  if t == "RDBO" then return true
  else return false , "not an RDBO" end
end

---initialize a new Remote Database Object
--@return  new RDB Object
--@usage  tokyotyrant = require'tokyotyrant'; rdbo = tokyotyrant.rdb.new(); rdbo:open(host, port)
function rdb.new ( )
  return setmetatable ( { } , mt )
end

---attach a socket to a database instance
--called automatically from rdb.open* functions
--@param sock  the socket to attach
--@return  true
function rdb:attach ( sock )
  assert ( rawget ( self , 'sock' ) == nil , "a socket is already attached" )

  rawset ( self , 'sock' , sock )
  return true
end

---open a tcp remote database connection and attach it to an instance
--@param host  the host string. (defaults to localhost)
--@param port  the port number (number or string) (defaults to '1978')
function rdb:open ( host , port )
  host = host or "localhost"
  port = port or 1978

  local sock = socket.tcp ( )

  assert ( sock:connect ( host , port ) )
  sock:setoption ( 'tcp-nodelay' , true )

  return self:attach ( sock )
end

---open a unix domain socket based remote database connection and attach it to an instance
--will fail if lua socket does not have unix extensions installed
--@param path  the path to the unix domain socket.
function rdb:openunix ( path )

  local sock = socket.unix ( )
  assert ( sock , "Unix sockets not available" )

  assert ( sock:connect ( path ) )

  return self:attach ( sock )
end

---close remote database connection
--@return  true
function rdb:close ( )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  sock:close ( )
  rawset ( self , 'sock' , nil )

  return true
end

---store a record
--if a record with same key already exists then it is overwritten
--@param key  the record key (coerced to string)
--@param val  the record value (coerced to string)
--@return  true or false
function rdb:put ( key , val )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  key = assert ( tostring ( key ) , "Invalid key" )
  val = assert ( tostring ( val ) , "Invalid value" )

  local req = struct.pack ( ">BBi4i4c0c0" , constants.MAGIC , constants.PUT , #key , #val , key , val )
  assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )
  return code == 0
end

---store a new record
--if record with same key exists then this has no effect
--@param key  the record key (coerced to string)
--@param val  the record value (coerced to string)
--@return  true or false
function rdb:putkeep ( key , val )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  key = assert ( tostring ( key ) , "Invalid key" )
  val = assert ( tostring ( val ) , "Invalid value" )

  local req = struct.pack ( ">BBi4i4c0c0" , constants.MAGIC , constants.PUTKEEP , #key , #val , key , val )
  assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )
  return code == 0
end

---concatenate a value at the end of an existing record
--if no record with given key exists then create new record
--@param key  the record key (coerced to string)
--@param val  the record value (coerced to string)
--@return  true or false
function rdb:putcat ( key , val )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  key = assert ( tostring ( key ) , "Invalid key" )
  val = assert ( tostring ( val ) , "Invalid value" )

  local req = struct.pack ( ">BBi4i4c0c0" , constants.MAGIC , constants.PUTCAT , #key , #val , key , val )
  assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )
  return code == 0
end

---concatenate a value at the end of an existing record and
--shift it left by (length of concatenation result - provided width)
--if no record with given key exists then create new record
--@param key  the record key (coerced to string)
--@param val  the record value (coerced to string)
--@param width  the desired record length
--@return  true or false
function rdb:putshl ( key , val , width )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  key = assert ( tostring ( key ) , "Invalid key" )
  val = assert ( tostring ( val ) , "Invalid value" )
  width = assert ( tonumber ( width or 0 ) , "Invalid width" )

  local req = struct.pack ( ">BBi4i4i4c0c0" , constants.MAGIC , constants.PUTSHL , #key , #val , width , key , val )
  assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )
  return code == 0
end

---store a record, with no response from the server
--if record with same key already exists then it is overwritten
--@param key  the record key (coerced to string)
--@param val  the record value (coerced to string)
--@return  true
function rdb:putnr ( key , val )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  key = assert ( tostring ( key ) , "Invalid key" )
  val = assert ( tostring ( val ) , "Invalid value" )

  local req = struct.pack ( ">BBi4i4c0c0" , constants.MAGIC , constants.PUTNR , #key , #val , key , val )
  assert ( sock:send ( req ) )

  return true
end

---remove a record
--@param key  the record key (coerced to string)
--@return  true or false
function rdb:out ( key )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  key = assert ( tostring ( key ) , "Invalid key" )

  local req = struct.pack ( ">BBi4c0" , constants.MAGIC , constants.OUT , #key , key )
  assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )
  return code == 0
end

---retrieve a record
--@param key  the record key (coerced to string)
--@return  record value or false
function rdb:get ( key )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  key = assert ( tostring ( key ) , "Invalid key" )

  local req = struct.pack ( ">BBi4c0" , constants.MAGIC , constants.GET , #key , key )
  assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )

  if code ~= 0 then return false end

  local vsiz = assert ( recvint32 ( sock ) )
  local vbuf = assert ( sock:receive ( vsiz ) )
  return vbuf
end

---retrieve multiple records
--@param recs  an array of keys
--@return  false or a table with an array part of values, and a hash part of key-value pairs
function rdb:mget ( recs )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  assert ( type ( recs ) == 'table' , "Invalid mget table" )

  local req = { struct.pack ( ">BBi4" , constants.MAGIC , constants.MGET , #recs ) }
  for i , k in ipairs ( recs ) do
    k = assert ( tostring ( k ) , "Invalid key" )
    req [ i * 2 ] = struct.pack ( ">i4" , #k )
    req [ i * 2 + 1 ] = k
  end
  req = tblconcat ( req )

  assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )
  local rnum = assert ( recvint32 ( sock ) )
  local res = { }
  for i = 1 , rnum do
    local ksiz , vsiz = assert ( recvint32 ( sock ) ) , assert ( recvint32 ( sock ) )
    local kbuf , vbuf = assert ( sock:receive ( ksiz ) ) , assert ( sock:receive ( vsiz ) )
    res [ i ] = vbuf
    res [ kbuf ] = vbuf
  end

  if code ~= 0 then return false end
  return res
end

---get the size of a record value
--@param key  the record key (coerced to string)
--@return  size or false
function rdb:vsiz ( key )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  key = assert ( tostring ( key ) , "Invalid key" )

  local req = struct.pack ( ">BBi4c0" , constants.MAGIC , constants.VSIZ , #key , key )
  assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )

  if code ~= 0 then return false end

  local vsiz = assert ( recvint32 ( sock ) )
  return vsiz
end

---initialize the iterator (used to access the key of every record)
--@return  true or false
function rdb:iterinit ( )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  local req = struct.pack ( ">BB" , constants.MAGIC , constants.ITERINIT )
  assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )
  return code == 0
end

---get the next key of the iterator
--the iterator will traverse the database in arbitrary order
--[[ It is possible to access every record by iteration of calling this method. It is allowed to update or remove records whose keys are fetched while the iteration. However, it is not assured if updating the database is occurred while the iteration. Besides, the order of this traversal access method is arbitrary, so it is not assured that the order of storing matches the one of the traversal access. ]]--
--@return  next key or false
function rdb:iternext ( )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  local req = struct.pack ( ">BB" , constants.MAGIC , constants.ITERNEXT )
  assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )

  if code ~= 0 then return false end

  local ksiz = assert ( recvint32 ( sock ) )
  local kbuf = assert ( sock:receive ( ksiz ) )
  return kbuf
end

---get forward matching keys
--note this will scan EVERY KEY in the database and may be  s l o w
--@param prefix  prefix of corresponding keys
--@param max  max number of keys to fetch, a negative number (or no number) means no limit.
--@return  false or array of keys of corresponding records
function rdb:fwmkeys ( prefix , max )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  prefix = assert ( tostring ( prefix ) , "Invalid prefix" )
  max = assert ( tonumber ( max or -1 ) , "Invalid maximum" )

  local req = struct.pack ( ">BBi4i4c0" , constants.MAGIC , constants.FWMKEYS , #prefix , max , prefix )
  assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )
  local knum = assert ( recvint32 ( sock ) )
  local res = { }
  for i = 1 , knum do
    local ksiz = assert ( recvint32 ( sock ) )
    local kbuf = assert ( sock:receive ( ksiz ) )
    res [ #res + 1 ] = kbuf
  end

  if code ~= 0 then return false end
  return res
end

---add an integer to a record
--if record exists, it is treated as an integer and added to
--else a new record is created with the provided value
--records are stored in binary format, and must be unpacked upon retrieval
--@param key  the record key
--@param num  the additional value. (defaults to 0)
--@return  sum or false
function rdb:addint ( key , num )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  key = assert ( tostring ( key ) , "Invalid key" )
  num = assert ( tonumber ( num or 0 ) , "Invalid number" )

  local req = struct.pack ( ">BBi4i4c0" , constants.MAGIC , constants.ADDINT , #key , num , key )
  assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )

  if code ~= 0 then return false end

  local sum = assert ( recvint32 ( sock ) )
  return sum
end

---add a double to a record
--if record exists, it is treated as a real number and added to
--else a new record is created with the provided value
--records are stored in binary format, and must be unpacked upon retrieval
--@param key  the record key
--@param num  the additional value. (defaults to 0)
--@return  sum or false
function rdb:adddouble ( key , num )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  key = assert ( tostring ( key ) , "Invalid key" )
  num = assert ( tonumber ( num or 0 ) , "Invalid number" )

  local integ = floor(num)
  local fract = floor( (num - integ) * 1e12 )

  local req = struct.pack ( ">BBi4i8i8c0" , constants.MAGIC , constants.ADDDOUBLE , #key , integ , fract , key )
  assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )

  if code ~= 0 then return false end

  local sum = assert ( recvint64 ( sock ) ) + assert ( recvint64 ( sock ) ) * 1e-12
  return sum
end

---call a function of the server-side lua extension
--@param name  the function name
--@param key  the key
--@param val  the value
--@param opts  "none" (or false/nil); "record" or "global"
--@return  false or called function's return value (a string)
function rdb:ext ( name , key , val , opts )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  name = assert ( tostring ( name ) , "Invalid column name" )
  key = assert ( tostring ( key ) , "Invalid key" )
  val = assert ( tostring ( val ) , "Invalid value" )
  if not opts or opts == "none" then
    opts = 0
  elseif opts == "record" then
    opts = constants.XOLCKREC
  elseif opts == "global" then
    opts = constants.XOLCKGLB
  else
    error ( "Invalid option" )
  end

  local req = struct.pack ( ">BBi4i4i4i4c0c0c0" , constants.MAGIC , constants.EXT , #name , opts , #key , #val , name , key , val )
  assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )

  if code ~= 0 then return false end

  local rsiz = assert ( recvint32 ( sock ) )
  local rbuf = assert ( sock:receive ( rsiz ) )
  return rbuf
end

---synchronize updated contents with the file and device
--@return  true or false
function rdb:sync ( )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  local req = struct.pack ( ">BB" , constants.MAGIC , constants.SYNC )
  assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )
  return code == 0
end

---optimize storage according to provided tuning params
--@return  true or false, error message
function rdb:optimize ( params )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  params = assert ( tostring ( params or "" ) , "Invalid parameter string" )

  local req = struct.pack ( ">BBi4c0" , constants.MAGIC , constants.OPTIMIZE , #params , params )
  assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )
  return code == 0
end

---remove all records
--@return  true or false
function rdb:vanish ( )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  local req = struct.pack ( ">BB" , constants.MAGIC , constants.VANISH )
  assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )
  return code == 0
end

---copy the database file to provided file path
--the db file will be kept in sync and not modified during copy
--@param path  the file path to copy to
--if path begins with '@' then trailing substring is executed as command line
--@return  true or false
function rdb:copy ( path )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  path = assert ( tostring ( path ) , "Invalid path" )

  local req = struct.pack ( ">BBi4c0" , constants.MAGIC , constants.COPY , #path , path )
  assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )
  return code == 0
end

---restore the database file of a remote database object from the update log
--@param path  the path of the update log directory
--@param ts  the beginning time stamp in seconds (microsecond resolution).
--@param opts  "none" (or false/nil) or "checkconsistency"
--@return  true or false
function rdb:restore ( path , ts , opts )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  path = assert ( tostring ( path ) , "Invalid path" )
  ts = assert ( tonumber ( ts ) , "Invalid time stamp" ) * 1e-3
  if not opts or opts == "none" then
    opts = 0
  elseif opts == "checkconsistency" then
    opts = constants.ROCHKCON
  else
    error ( "Invalid option" )
  end

  local req = struct.pack ( ">BBi4i8i4c0" , constants.MAGIC , constants.RESTORE , #path , ts , opts , path )
  assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )
  return code == 0
end

---set the replication master of a remote database object.
--@param host  name or the address of the server.
--@param port  port number (defaults to 1978)
--@param ts  the beginning time stamp in seconds (microsecond resolution).
--@param opts  "none" (or false/nil) or "checkconsistency"
--@return  true or false
function rdb:setmst ( host , port , ts , opts )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  host = assert ( tostring ( host ) , "Invalid host" )
  port = assert ( tonumber ( port or 1978 ) , "Invalid port" )
  ts = assert ( tonumber ( ts ) , "Invalid time stamp" ) * 1e-3
  if not opts or opts == "none" then
    opts = 0
  elseif opts == "checkconsistency" then
    opts = constants.ROCHKCON
  else
    error ( "Invalid option" )
  end

  local req = struct.pack ( ">BBi4i4i8i4c0" , constants.MAGIC , constants.SETMST , #host , port , ts , opts , host )
  assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )
  return code == 0
end

---get number of records
--( limited to Lua's number type precision )
--@return  false or number of records
function rdb:rnum ( )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  local req = struct.pack ( ">BB" , constants.MAGIC , constants.RNUM )
    assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )
  local rnum = assert ( recvint64 ( sock ) )

  if code ~= 0 then return false end
  return rnum
end
mt.__len = rdb.rnum

---get size of database
--( limited to Lua's number type precision )
--@return  false or size of database
function rdb:size ( )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  local req = struct.pack ( ">BB" , constants.MAGIC , constants.SIZE )
    assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )
  local rnum = assert ( recvint64 ( sock ) )

  if code ~= 0 then return false end
  return rnum
end

---get status string from remote database server
--( string is in 'tab separated values' format )
--@return  false or table of statistics
function rdb:stat ( )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  local req = struct.pack ( ">BB" , constants.MAGIC , constants.STAT )
  assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )
  local ssiz = assert ( recvint32 ( sock ) )
  local sbuf = assert ( sock:receive ( ssiz ) )

  if code ~= 0 then return false end

  local res = { }
  for key , value in sbuf:gmatch ( "([^\t]*)\t([^\n]*)\n" ) do
    res [ key ] = value
  end
  return res
end

---call a versatile function for miscellaneous operations
--@param name  All databases support "put", "out", "get", "putlist", "outlist", and "getlist".
--"put" is to store a record. It receives a key and a value, and returns an empty list.
--"out" is to remove a record. It receives a key, and returns an empty list.
--"get" is to retrieve a record. It receives a key, and returns a list of the values.
--"putlist" is to store records. It receives keys and values one after the other, and returns an empty list.
--"outlist" is to remove records. It receives keys, and returns an empty list.
--"getlist" is to retrieve records. It receives keys, and returns keys and values of corresponding records one after the other.
--or 'setindex', 'search', or 'genuid' for table db
--@param args  an array containing arguments
--@param opts  "none" (or false/nil) or "noupdatelog"
--@return  array of results or nil, error message
function rdb:misc ( name , args , opts )
  local sock = rawget ( self , "sock" )
  assert ( sock , "No socket attached" )

  name = assert ( tostring ( name ) , "Invalid column name" )
  args = args or { }
  assert ( type ( args ) == "table" )
  if not opts or opts == "none" then
    opts = 0
  elseif opts == "noupdatelog" then
    opts = constants.MONOULOG
  else
    error ( "Invalid option" )
  end

  local req = { struct.pack ( ">BBi4i4i4c0" , constants.MAGIC , constants.MISC , #name , opts , #args , name ) }
  for i, arg in ipairs ( args ) do
    assert ( type ( arg ) == "string" )
    req [ i + 1 ] = struct.pack ( ">i4c0" , #arg , arg )
  end
  req = tblconcat ( req )

  assert ( sock:send ( req ) )

  local code = assert ( recvuchar ( sock ) )
  local rnum = assert ( recvint32 ( sock ) )
  local res = { }
  for i = 1 , rnum do
    local esiz = assert ( recvint32 ( sock ) )
    local ebuf = assert ( sock:receive ( esiz ) )
    res [ i ] = ebuf
  end

  if code ~= 0 then return false end
  return res
end

---A lua style iterator factory: iterates over each key-value pair in a database
--Do not call iterinit on the same RDBO object while in the loop.
--@return an iterator , the RDBO
--@usage for k , v in rdb:pairs ( ) do print(k,v) end
function rdb:pairs ( )
  assert ( self:iterinit ( ) )
  return function ( self )
      local k = self:iternext ( )
      if not k then
        return nil
      end
      local v = assert ( self:get ( k ) )
      return k , v
    end , self
end
mt.__pairs = rdb.pairs

local tbldb = setmetatable  ( { } , classmt )
local tbldbmt = {
  __index = function ( t , k )
    return tbldb [ k ] or rdb [ k ]
  end
}
---initialize a new Remote Table Database Object
--@return  new RDBTBL Object
function tbldb.new ( )
  return setmetatable ( { } , tbldbmt )
end

---store a record
--( overwrite if key exists )
--@param pkey  the primary key
--@param cols  table of columns
--@return  true or false
function tbldb:put ( pkey , cols )
  pkey = assert ( tostring ( pkey ) , "Invalid primary key" )
  assert ( type ( cols ) == 'table' , "'cols' must be a table of columns" )

  local args , nexti = { pkey } , 2
  for k , v in pairs ( cols ) do
    args [ nexti ] = k
    args [ nexti + 1 ] = v
    nexti = nexti + 2
  end

  return not not self:misc ( "put" , args )
end

---store a record if key does not already exist, else do nothing
--@param pkey  the primary key
--@param cols  table of columns
--@return  true or false
function tbldb:putkeep ( pkey , cols )
  pkey = assert ( tostring ( pkey ) , "Invalid primary key" )
  assert ( type ( cols ) == 'table' , "'cols' must be a table of columns" )

  local args , nexti = { } , 1
  for k , v in pairs ( cols ) do
    args [ nexti ] = k
    args [ nexti + 1 ] = v
    nexti = nexti + 2
  end
  return not not self:misc ( "putkeep" , args )
end

---concatenate columns of an existing record or create a new record
--@param pkey  primary key
--@param cols  a table of columns
--@return  true or false
function tbldb:putcat ( pkey , cols )
  pkey = assert ( tostring ( pkey ) , "Invalid primary key" )
  assert ( type ( cols ) == 'table' , "'cols' must be a table of columns" )

  local args , nexti = { } , 1
  for k , v in pairs ( cols ) do
    args [ nexti ] = k
    args [ nexti + 1 ] = v
    nexti = nexti + 2
  end
  return not not self:misc ( "putcat" , args )
end

---remove a record
--@param pkey  the primary key
--@return  true or false
function tbldb:out ( pkey )
  pkey = assert ( tostring ( pkey ) , "Invalid primary key" )

  local args = { pkey }
  return not not self:misc ( "out" , args )
end

---retrieve a record
--@param pkey  the primary key
--@return  a table of columns
function tbldb:get ( pkey )
  pkey = assert ( tostring ( pkey ) , "Invalid primary key" )

  local args = { pkey }

  local res = assert ( self:misc ( "get" , args ) )
  local cols = { }
  for i = 1 , #res , 2 do
    cols [ res [ i ] ] = res [ i + 1 ]
  end
  return cols
end

--[[-retrieve multiple records
--given a table containing an array of keys, add a hash of key-value pairs
--( values being columns )
--keys in the array with no corresponding value will be removed from the array
--( understand that the table given is modified in place )
--NOTE: due to protocol restriction, this method cannot handle records with
--binary columns including the "\0" character.
--@param recs  an array of keys
--@return  number of retrieved records or -1, error message
function RDBTBL:mget(recs)
  local res, err = RDB.mget(self, recs)
  if res == -1 then return -1, err end
  for k,v in pairs(res) do
    local cols = {}
    local func, str = strsplit(v, '\%z')
    while true do
      local kk, vv = func(str), func(str)
      if kk then cols[kk] = vv
      else break end
    end
    recs[k] = cols
  end
  return res
end--]]


local indextypes = {
  LEXICAL = 0 ;
        DECIMAL = 1 ;
        TOKEN = 2 ;
        QGRAM = 3 ;
        OPT   = 9998 ;
        VOID  = 9999 ;
        KEEP  = 2^(24-1) ;
}
---set a column index
--@param name  the column name.
--if the name of an existing index is specified, then the index is rebuilt.
--an empty string means the primary key.
--@param itype  the index type: "lexical" , "decimal" , "token" , "qgram" , "opt" , "void" or "keep"
--@return  true or false
function tbldb:setindex ( name , itype )
  name = assert ( tostring ( name ) , "Invalid column name" )
  itype = assert ( type ( itype ) == "string" and indextypes [ itype:upper ( ) ] , "Invalid index type" )

  return not not self:misc ( "setindex" , { name , tostring ( itype ) } )
end

---generate a unique id number
--@return  unique id number or -1, error message
function tbldb:genuid ( )
  local res = assert ( self:misc ( "genuid" , { } ) )
  return res [ 1 ]
end

---execute a search
--@param query  a query object
--@return  false or array of primary keys of corresponding records
function tbldb:search ( query )
  return self:misc ( "search" , query )
end

---remove each corresponding record
--@param query  a query object
--@return  true or false
function tbldb:searchout ( query )
  query [ #query + 1 ] = "out"
  return not not self:misc ( "search" , query )
end

---get the count of corresponding records
--@param query  a query object
--@return  count or false
function tbldb:searchcount ( query )
  query [ #query + 1 ] = "count"
  return self:misc ( "search" , query ) [ 1 ] or false
end

local query = setmetatable ( { } , classmt )
local querymt = { __index = query }

---initialize a new query object
--@return  new RDBQRY object
function query.new ( )
  return setmetatable ( { } , querymt )
end

local ops = {
  STREQ = 0 ;   --string is equal to
  STRINC = 1 ;  --string is included in
  STRBW = 2 ;   --string begins with
  STREW = 3 ;   --string ends with
  STRAND = 4 ;  --string includes all tokens in
  STROR = 5 ;   --string includes at least one token in
  STROREQ = 6 ; --string is equal to at least one token in
  STRRX = 7 ;   --string matches regular expressions of
  NUMEQ = 8 ;   --number is equal to
  NUMGT = 9 ;   --number is greater than
  NUMGE = 10 ;  --number is greater than or equal to
  NUMLT = 11 ;  --number is less than
  NUMLE = 12 ;  --number is less than or equal to
  NUMBT = 13 ;  --number is between two tokens of
  NUMOREQ = 14 ;--number is equal to at least one token in
  FTSPH = 15 ;  --full-text search with the phrase of
  FTSAND = 16 ; --full-text search with all tokens in
  FTSOR = 17 ;  --full-text search with at least one token in
  FTSEX = 18 ;  --f-text search with the compound expression of
}
---add a narrowing condition
--@param name  specifies column name. empty string indicates the primary key. (default primary key)
--@param op  operation type:
--"streq" , "strinc" , "strbw" , "strew" , "strand" , "stror" , "stroreq" , "strrx"
--"numeq" , "numgt" , "numge" , "numlt" , "numle" , "numbt" , "numoreq"
--"ftsph" , "ftsand" , "ftsor" , "ftwex"
--@param expr  specifies an operand expression
--@param negate boolean: negate the operation
--@param noindex boolean
--@return  self
function query:addcond ( name , op , expr , negate , noindex )
  name = assert ( tostring ( name or "" ) , "Invalid column name" )
  op = assert ( type ( op ) == "string" and ops [ op:upper ( ) ] , "Invalid operation" )
  if negate then op = op + 2^(24-1) end
  if noindex then op = op + 2^(25-1) end
  expr = assert ( tostring ( expr ) , "Invalid expression" )

  self [ #self + 1 ] = "addcond\0" .. name .. "\0" .. op .. "\0" .. expr
  return self
end

local orders = {
  STRASC = 0 ; --string ascending
  STRDESC = 1 ; --string descending
  NUMASC = 2 ; --number ascending
  NUMDESC = 3 ; --number descending
}
---set result order
--@param name  specifies column name. empty string indicates the primary key. (default primary key)
--@param otype  specifies the order type: "strasc" , "strdesc" , "numasc" or "numdesc" (default string ascending)
--@return  self
function query:setorder ( name , otype )
  name = assert ( tostring ( name or "" ) , "Invalid column name" )
  otype = otype or "STRASC"
  otype = assert ( type ( otype ) == "string" and orders [ otype:upper ( ) ] , "Invalid sort order type" )

  self [ #self + 1 ] = "setorder\0" .. name .. "\0" .. otype
  return self
end

---set maximum number of records for the result
--@param max  the maximum number of records. nil or negative means no limit
--@param skip the number of skipped records. nil or negative means none skipped
--@return  self
function query:setlimit ( max , skip )
  max = assert ( tonumber ( max or -1 ) , "Invalid max" )
  skip = assert ( tonumber ( skip or -1 ) , "Invalid skip" )

  self [ #self + 1 ] = "setlimit\0" .. max .. "\0" .. skip
  return self
end


--[[---get records corresponding to search
--due to protocol restriction, method cannot handle records with binary cols
--including the '\0' character
--@param names  specifies an array of column names to fetch.
--empty string means the primary key
--nil means fetch every column
--@return  array of column hashes of corresponding records
--or {}, error message
function RDBQRY:searchget ( rdb , names )
  if type(names) ~= 'table' then
    error("'names' must be an array of column names")
  end
  local args = {}
  if #names > 0 then
    args[1] = "get\0" .. tblconcat(names, '\0')
  else args[1] = "get" end
  local res, err = rdb:misc('search', args, "noupdatelog" )
  if not res then return {}, err end
  for i, v in pairs(res) do
    local cols = {}
    local func, str = strsplit(v, '\%z')
    while true do
      local kk, vv = func(str), func(str)
      if kk then cols[kk] = vv
      else break end
    end
    res[i] = cols
  end
  return res
end

--]]

--[[---Rici Lake's string splitter
function strsplit(str, pat)
  local st, g = 1, str:gmatch("()("..pat..")")
  local function getter(str, segs, seps, sep, cap1, ...)
    st = sep and seps + #sep
    return str:sub(segs, (seps or 0) - 1), cap1 or sep, ...
  end
  local function splitter(str, self)
    if st then return getter(str, st, g()) end
  end
  return splitter, str
end--]]

return { rdb = rdb , tbldb = tbldb , query = query }

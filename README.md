eve_sde
=====
Tool for creating ets datasets from CCP SDE.

Usage
-----
after starting sde_server it will parse or load(if alredy exists) tables based on config
  - TODO config description

Tables owner
-----
Tables are protected (there are not much reasons to modify sde).
Tranfer ownership if you need to modify them:
```erlang
eve_sde:take_table(TableName).
eve_sde:take_table(ServerName, TableName).
```
in case you need to tranfer table to antoher process:
```erlang
eve_sde:give_table(TableName, PID).
eve_sde:give_table(ServerName, TableName, PID).
```
and then handle message
```erlang
{'ETS-TRANSFER',Tab,FromPid,GiftData}
```
Dont forget to return them (should be called from owner process):
```erlang
eve_sde:return_table(TableName).
eve_sde:return_table(ServerName, TableName).
```

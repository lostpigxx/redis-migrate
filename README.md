# redis-migrate
穿透codis-proxy，直接将数据迁移到codis分片

### 配置文件说明
目前配置文件里不能写注释（懒得弄了），就在这里说明一下
```
# codis集群的slot总数，默认是1024
slot_num 1024

# 内部并发的线程数，看情况增加吧
parallel 32

# 源端信息 - ip 端口 密码
# 只能有一个源端
source 127.0.0.1 6379 a

# 目的端信息 - ip 端口 密码 slot-min slot-max
# 注意，slot左右都是闭区间，并且总的范围要是[0, slot_num-1]
# 多个目的端写成多行即可
dest 127.0.0.1 6380 a 0 300
dest 127.0.0.1 6380 a 301 600
dest 127.0.0.1 6380 a 601 900
dest 127.0.0.1 6380 a 901 1023
```
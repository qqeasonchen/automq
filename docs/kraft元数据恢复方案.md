# 背景：wemq-kafka集群对接单IDC1的ceph集群，且ceph执行IDC1异步复制到IDC2，假设单IDC异步复制有延迟且该IDC异常宕机
#   希望在IDC2尽可能恢复数据多的数据供业务消费

# 操作步骤：
1.定时执行kraft元数据(log.dirs目录完整文件)的完整备份，打包压缩包写入主Ceph集群(kraft-metadata-202509261550.zip)
2.备Ceph集群定时去主集群拉取主Ceph集群执行日志，并在备集群顺序执行
3.因为Ceph异步复制数据是顺序执行的，kraft-metadata-202509261550.zip快照在备集群存在，则备集群依赖的数据也同样在备集群存在
4.执行IDC2 broker数据恢复，删除原有kraft元数据目录(log.dir目录)，更换wal数据对应的bucket(或删除历史wal数据复用wal的bucket)
5.broker正常启动，原有topic存在，原有topic数据能正常消费到

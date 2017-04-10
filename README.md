# 说明
工作中用到了DataX这个来自阿里巴巴的非常不错的工具，其具体介绍可直接访问[这里](https://github.com/alibaba/DataX)，可是有一点，其增量同步功能不是很好用，好像也有阵子没更新了。因此此版基于阿里巴巴的DataX进行了一些简单修改，增加了增量同步和实时同步的功能。修改仅为满足自用需求，尽量但没有经过完全覆盖测试（主要跑了mysql2odps），恐非最佳方案，仅供有兴趣者参考。

# DataX

DataX 是阿里巴巴集团内被广泛使用的离线数据同步工具/平台，实现包括 MySQL、Oracle、SqlServer、Postgre、HDFS、Hive、ADS、HBase、OTS、ODPS 等各种异构数据源之间高效的数据同步功能。


# Features

DataX本身作为数据同步框架，将不同数据源的同步抽象为从源头数据源读取数据的Reader插件，以及向目标端写入数据的Writer插件，理论上DataX框架可以支持任意数据源类型的数据同步工作。同时DataX插件体系作为一套生态系统, 每接入一套新数据源该新加入的数据源即可实现和现有的数据源互通。

# 如何设定增量同步

很简单，在reader参数中增加"incrementalSyncColumn": "your_column_name_here_XXX"即可，要求该字段必须也在column参数中列出。读取时只会读取上次checkpoint之后的数据。支持增量同步的字段的数据类型包括各类bigint，int，long，double等数值型字段，也支持datetime，date，timestamp等时间型字段，会自动识别。
去掉该字段即恢复。
另外注意：如果writer参数中指明了"truncate": true，则写入时会只剩下增量部分，所以建议增量同步时设为false


# 源码阅读提示

Engine.start->JobContainer.start->split->doReaderSplit->MysqlReader.split->ReaderSplitUtil.doSplit-> SingleTableSplitUtil.buildQuerySql->schedule->AbstractScheduler.schedule->ProcessInnerScheduler.startAllTaskGroup->ExecutorService.execute()->while(true) -> containerCommunicator.collect()->多次report()->State.SUCCEEDED->break;
                                               State.FAILED>break;
               
               WriterRunner->OdpsWriter.prepare->startWrite(BufferedRecordExchanger.getFromReader->receive->Channel.pullAll->MemoryChannel.doPullAll->updateCheckpoint->saveCheckpoint

		ReaderRunner->MysqlReader.startRead->CommonRdbmsReader.startRead->transportOneRecord-> BufferedRecordExchanger.sendToWriter->flush->Channel.pushAll->MemoryChannel.doPushAll

Engine.java读配置时，增加loadCheckpoint方法（加载checkpoint文件内容到job.content[0].reader.parameter.checkpoint={})
SingleTableSplitUtil.buildQuerySql(重载方法新增参数)
把完整SQL拼装好后放在job.content[0].reader.parameter.querySql参数值中。(这个参数本已存在，作为Reader的执行SQL)
把最新resultset的 maxvalue@columnIndex 暂存到MemoryChannel.checkpoint变量中，再存到配置job.content[0].reader.parameter.checkpoint中

 
# 修改点
core:
    WriteRunner.java
    MemoryChannel.java
    BufferedRecordExchanger.java
    CoreConstant.java(常量)
    Engine.java
plugin-rdbms-util
    ReaderSplitUtil.java
    SingleTableSplitUtil.java
    Key.java(常量)
odpsreader
    pom.xml(升级了odps-sdk-core依赖到0.24.0-public版本)
odpswriter
    pom.xml(升级了odps-sdk-core依赖到0.24.0-public版本)

# Quick Start

##### 回头找个地方提供一个编译好的版本下载。 Download [DataX下载地址](http://datax-opensource.oss-cn-hangzhou.aliyuncs.com/datax.tar.gz)




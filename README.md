# FBDP作业五：通过MapReduce实现Redditnews的WordCount并排序

## 要求

在HDFS上加载Reddit WorldNews Channel热点新闻标题数据集（RedditNews.csv），该数据集收集了2008-06-08至2016-07-01每日的TOP25热点财经新闻标题。编写MapReduce程序进行词频统计，并按照单词出现次数从大到小排列，输出（1）数据集出现的前100个高频单词；（2）每年热点新闻中出现的前100个高频单词。要求忽略大小写，忽略标点符号，忽略停词（stop-word-list.txt）。输出格式为"<排名>：<单词>，<次数>“，输出可以根据年份不同分别写入不同的文件，也可以合并成一个文件。



【注1】原始数据可以根据情况做预处理；



【注2】作业提交方式：git仓库地址或者相关文件的zip包

git仓库目录组织建议：

\- project name （例如wordcount）

|  +-- src

|  +-- target

|  +-- output

|  |  +-- result (输出结果文件)

|  +-- pom.xml

|  +-- .gitignore（target目录下只保留jar文件，并忽略其它无关文件）

|  +-- readme.md （对设计思路，实验结果等给出说明，并给出提交作业运行成功的WEB页面截图。可以进一步对性能、扩展性等方面存在的不足和可能的改进之处进行分析。）

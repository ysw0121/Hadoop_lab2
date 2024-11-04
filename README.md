# Hadoop_lab2

## 仓库说明
1. 本仓库文件命名按照`task1,2,3,4`命名，只保留在`docker`上运行的结果和`jar`文件，在`target`文件夹中。本次提交忽略了其他`class`文件。虚拟机本地未进行编译。
2. 输出仍保留一些失败文件，在`output/failed_output`中，其他在`output`中的是成功输出结果。
3. 本次思路仍沿用作业5的单文件运行思路，没有`pom.xml`文件。


## 关键命令
与[作业5](https://github.com/ysw0121/Hadoop_homework)基本相同

## 任务1
根据`user_balance_table`表中的数据，统计所有⽤户每⽇的资⾦流⼊与流出情况。资⾦流⼊意味着申购⾏为，资⾦流出为赎回⾏为。每笔交易的资⾦流⼊和流出量分别由字段`total_purchase_amt`和`total_redeem_amt`表示。数据中的缺失值，将视为零交易。输出格式：`<⽇期> TAB <资⾦流⼊量>,<资⾦流出量>`
### 设计思路

### 运行结果

### 可能的改进之处

## 任务2
基于任务1的结果，统计⼀周七天中每天的平均资⾦流⼊与流出情况，并按照资⾦流⼊量从⼤到⼩排序。输出格式：`<weekday> TAB <资⾦流⼊量>,<资⾦流出量>`
### 设计思路

### 运行结果

### 可能的改进之处

## 任务3
根据`user_balance_table`表中的数据，统计每个⽤户的活跃天数，并按照活跃天数降序排列。当⽤户当⽇有直接购买（`direct_purchase_amt`字段⼤于0）或赎回⾏为（`total_redeem_amt`字段⼤于0）时，则该⽤户当天活跃。输出格式：`<⽤户ID> TAB <活跃天数>`
### 设计思路

### 运行结果

### 可能的改进之处

## 任务4
⽤户的交易⾏为（如：余额宝或银⾏卡的购买或赎回，⽤户的消费情况等）受到很多因素的影
响。如：⽤户特性（参考⽤户信息表`user_profile_table`），当前利率（参考⽀付宝收益率
表`mfd_day_share_interest`以及银⾏利率表`mfd_bank_shibor`）。现在从上述其他的表中⾃⾏选取研究对象进行统计，根据统计结果（类似于上⾯三个任务的结果）阐述某⼀因素对⽤户交易⾏为的影响。
### 设计思路

### 运行结果

### 可能的改进之处

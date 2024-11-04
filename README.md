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
1. Mapper类 (`flowMapper`)：读取输入文件的每一行数据。在在前期实验中发现最后一行出现`report_date	0,0`，因此代码中加入了跳过第一行表头行的逻辑判断。接着将每一行的数据按逗号分割成3个字段。分别代表`提取日期`、`购买金额`和`赎回金额`字段，并将空值替换为 "0"。将处理后的数据以`<日期\t购买金额,赎回金额>`的形式传给`Reducer`。
2. Reducer 类 (`flowReducer`)：接收`Mapper`输出的数据。按日期分组，累加购买金额和赎回金额。将累加结果以`<日期\t总购买金额,总赎回金额>`的形式输出。在实际运行中多次出现`java.lang.NumberFormatException`异常，导致无法输出结果，因此代码中增加了`try-catch`跳过处理。

### 运行结果

### 可能的改进之处
1. 错误处理：在`flowMapper`中，对输入格式不正确的数据，可以先进行`NumberFormatException`处理，跳过该行数据。
2. 性能优化:如果输入数据量很大，可以考虑使用更高效的数据结构或算法来提高处理速度。比如可以考虑使用 `Combiner`来减少网络传输的数据量。

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

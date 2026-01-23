## Project Overview

1. 这个项目，用来实现一个基于SPDK的KV engine
2. 提供基本的create, open, put, get, del 等几个接口
3. 输入的key 为uint64， value 为已经多个各自符合SPDK 内存对齐要求地址
4. 所有的io 接口，均为异步IO 方式。
5. 假定value的平均长度大于4KB
6. 不允许写入长度为0的数据

## Development Status

This repository is currently empty and in initial setup phase. This file should be updated once the project structure is established with:
- build with cmake
- use .clang-format for c++ code style
- make sure compile pass

## Common Development Considerations

keep in mind:
- 这个engine，会被集成到一个单线程polling的环境中。因此不需要考虑线程安全问题。但是需要确保IO 链路上，所有的操作都是异步的
- 基本思路，这个engine 基于spdk的blob 相关操作。
- io的读写操作，参考bitcask的思想。
- 写入的时候，再最新的data文件中，追加写入数据，并将索引信息，存放再内存的hash表中
- 读的时候，通过查找索引，获取到data的文件id 和offset、length 信息，再映射spdk的blob id 信息，然后从对应的blob 中读取数据
- 删除的时候，与写入类似。先再data文件中，写入删除标记，再将内存索引删除或者标记删除
- 重写的时候，遍历索引文件，检查数据的有效性，跳过已经删除或者stale的数据，将有效的数据，写到新的文件的末尾。
- 每个engine， 预期平均value的长度大于4KB。以nvme 单盘8TB为目标的话，最多存放20亿左右的记录
- 假定每条索引需要占用16B的内存空间，因此需要消耗(8TB/4KB) * 16B = 32GB
- 考虑spdk的blob id 的和递增关系问题，因此需要设计一个自定义的文件id，将文件id 映射到blob的id.
- 每次用户的写入，需要对齐到固定的长度单位，比如512B。写入的数据为1025，实际占用的空间长度为1024 + 512
- 内存索引，考虑主要有如下几个字段
- file_id: 用于表示位于哪个data文件中，进而映射到哪个blob id 中。
- offset_index: data文件内部的偏移, 即位于第几个对齐单位。不是绝对地址。 
- value_length: value占用页面的个数。即mem 索引中表示占用几个page (4096)
- value_length 是否需要描述占用具体的页面数量，需要再讨论。假设默认支持16个page以内，都上来后，如果超过16个page，可以根据数据头部中记录的详细信息，来判断是否需要再次读取。
- file_id 具体需要多少靠，需要考虑如下几个因素
    - 每个打开的blob占用的meta信息。
    - 单个engine支持的最大空间
    - 空间回收效率
- 假设每个文件大小为8GB， 单个engine大小为4096GB，这需要512个文件, 文件id 需要9bit来描述，如果需要支持8192GB的空间，则需要10bit描述
    - 假定文件大小为8GB，对齐单位为512B，则在文件内部描述的话，需要8 * 1024 * 1024 * 1024 / 512, 需要24bit 来进行寻址。
    - 假定文件大小为8GB，对齐单位为4096B，则文件内部的描述， 8 * 1024 * 1024 * 1024 / 4096, 需要21bit寻址
- 对齐单位大小，需要考虑全路径zero copy的对齐问题
- 将整个engine划分成 superblock, MemIndex Area，blob data file 等3个区域。
- superblock 用于存放engine的全局信息
- MemIndexArea 用于存放序列化的内存索引信息；此索引信息分为2个区域，周期、写入两统计两种出发dump的时机。分为A B 两个区域。其中一个区域为最新数据，加载的时候，加载2个地方的meta信息，通过版本比较，获取最新版本。
- MemIndex的具体实现，需要考虑几个因素
    - 开放寻址
    - 创建时，确定好内存空间，避免中间分配内存
    - 基于固定内存空间，能够将内存dump 整块dump到MemIndexArea中，并支持快速反序列化，避免启动时逐条加载。
- 写入的时候，需要考虑在异步IO的时候，同一条key 写入两次。 假设写入 key->valueA, 又写入了key->valueB
  valueA先写盘，然后再写入valueB
  但是写入valueB的回调先执行，在内存索引中写入key 指向valueB
  然后写入valueA的回调再执行，在内存索引中写入key 指向valueA
  重启后，顺序加载，key 又指向valueB, 前后不一致
- 需要在nvme 上规划固定的空间，用于将data文件对应的索引信息，固定到对应的索引文件中，这样在rewrite的时候，只需要遍历这段索引文件即可。
  注意，索引文件需要考虑回收。
  需要考虑，索引文件，采用周期性dump或者切换文件的时候，进行dump。
  在启动的时候，需要将未dump的文件，加载起来，并根据情况进行dump。


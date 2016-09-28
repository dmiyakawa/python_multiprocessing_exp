# これは何

Python 3 + multiprocessing + threadingのデモ

## 詳しく

2つの引数`in_dir_path`, `out_dir_path`を受取り、
`in_dir_path`のファイルツリー構造と同じファイルツリー構造を
`out_dir_path`に作る。
`out_dir_path`に作られるファイルは本物のコピーではなく、
単なる1KBの無意味なプレーンテキストファイル。
拡張子は元のファイル名と同じまま。

シングルプロセスで作るのではなく、
あえてmultiprocessing.Processとthread.Threadを使用して
Pythonにおけるマルチプロセスの挙動を検証する。

`out_dir_path`に触れるのは`Worker`と呼ばれる別プロセスとなり、
元プロセスではない。
`Worker`から作成されたファイルのパスがやはりQueueで返されるが、
これを元プロセスでシリアルに処理しようとするとデッドロックを起こすため、
`Receiver`を定義。
この場合、`Receiver`はThreadでもProcessでも良いはずなので、
それぞれの挙動の差異を見るために切替可能とした。
結果として、`Receiver`が実行するタスク周りで`KeyboardInterrupt`の
伝播のされ方や停止時のThread、Processの微妙な違いが分かることになった。


実行時の、特にKeyboardInterrupt例外の処理についても
ThreadやProcessが作業途中でデッドロックしないように
実装しようとするとどうなるか、確認する。
ただし極稀に現状でも停止する可能性がある模様……

マルチプロセス構成においてLoggerを集約するためのQueueHandlerも使用してみた。
LoggerはCPythonのGILベースのマルチスレッド構成ではスレッドセーフだが
マルチプロセスにおいてはそうではないため、集約する必要がある。
そこで、想定するジョブ(特に今回の`Worker`)上のLoggerのHandlerについては
多少の工夫が必要になる模様。

http://docs.python.jp/3/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes

なお、Logger周りはベスト・プラクティスがよくわからないで、
本実装はかなりアドホックな点に注意。

後もう一点。本プログラムはUnix系のみならず、
Windows上でも動作することを意識している。


## メモ

対象ディレクトリ内のファイルが10000個程度になると、
Queueとそこからデータを受け取るプロセスを正しく実装しないと
かなり簡単にデッドロックを起こす。
またKeyboardInterruptの挙動はThreadとProcessで当然のことながら異なり

# ライセンス

ISC

Copyright (c) 2016, Daisuke Miyakawa

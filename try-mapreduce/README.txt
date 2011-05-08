概要：

とにかくMapReduceプログラムを動かしてみるという事を目的とした
MapReduceのサンプルプログラムです。

前提
----
Eclipse上で実行することを前提としています。
（単純に java コマンドでも動作するとは思います）
また、制限事項にもありますが、分散環境で実行する際は
ソースファイルの修正等が必要になります。

ディレクトリ構成
----------------
try-mapreduce
　└input			MapReduceプログラムの入力ディレクトリ
　　└Department	部門ファイル
　　└Employee		従業員ファイル
　　└WordCount		WordCountプログラム用の入力ファイル
　└lib				ライブラリ
　　└ext			hadoopに含まれない外部ライブラリ
　└src				ソースファイル
　└target			出力ディレクトリ
　　└output		MapReduceプログラムの出力ディレクトリ

サンプル一覧
------------
01～03はWordcount関連です。
04以降は部門ファイルと従業員ファイルを操作する
MapReduceプログラムです。

ソースファイルを読む際は、以下の順番に沿って読むことを推奨します。

01.WordCount
02.WordCountNew
03.WordCountOld
04.AverageAgeOfEmployee
05.AverageAgeOfDepartment
06.SortByAgeUsingHashPartitioner
07.SortByAgeUsingTotalOrderPartitioner
08.SortByDeptAndAgeUsingComparator
09.SortByDeptAndAgeUsingSecondarySort
10.MergeByDepartmentUsingMultipleInputs
11.JoinWithDeptNameUsingReduceSideJoin
12.JoinWithDeptNameUsingMapSideJoin
13.JoinWithDeptNameUsingDistributedCacheFile

制限事項
--------
気軽に動作させることを優先させたため、実際に分散環境で動作させる際は
各ソースファイルのmainメソッドの先頭でハードコーディングしている
引数部分のコードを除去する必要があります。
また、一部のプログラムにおいてhadoop本体に含まれない
外部ライブラリを利用しています。
そのため、jarファイルとしてまとめる際は、「lib/ext」フォルダに含まれる
ライブラリを含めるようにして下さい。

今後について
------------
ユニットテストが入っていませんが、今後追加する予定です。

その他
------
何かありましたら、以下にコメントをお願い致します。
http://code.google.com/p/try-hadoop-mapreduce-java/
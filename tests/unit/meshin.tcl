start_server {
    tags {"meshin"}
    overrides {
        "list-max-ziplist-value" 16
        "list-max-ziplist-entries" 256
    }
} {
    test {GROUPSUM} {
	assert_equal 1 [r rpush keylist 1]
	assert_equal 2 [r rpush keylist 2]
	assert_equal 3 [r rpush keylist 3]
	assert_equal 1 [r sadd keys1 1]
	assert_equal 1 [r sadd keys1 2]
	assert_equal 1 [r sadd keys1 3]
	assert_equal 1 [r sadd keys2 3]
	assert_equal 1 [r sadd keys3 1]
	assert_equal 1 [r sadd keys3 2]
	assert_equal OK [r set s1 1]
	assert_equal OK [r set s2 10]
	assert_equal OK [r set s3 100]
	assert_equal OK [r set ss1 2]
	assert_equal OK [r set ss2 20]
	assert_equal OK [r set ss3 200]
	assert_equal OK [r set sss1 3]
	assert_equal OK [r set sss2 30]
	assert_equal OK [r set sss3 300]
	assert_equal 9 [r groupsum sumlist keylist keys* s* ss* sss*]
	assert_equal {111 222 333 100 200 300 11 22 33} [r lrange sumlist 0 -1]
    }

    # We need a value larger than list-max-ziplist-value to make sure
    # the list has the right encoding when it is swapped in again.
    array set largevalue {}
    set largevalue(ziplist) "hello"
    set largevalue(linkedlist) [string repeat "hello" 4]

    test {LdUNIQUE a list - ziplist} {
	assert_equal 1 [r lpush ulist1 1]
	assert_equal 2 [r lpush ulist1 2]
	assert_equal 3 [r lpush ulist1 3]
	assert_equal 4 [r lpush ulist1 4]
	assert_equal 5 [r lpush ulist1 1]
	assert_equal 6 [r lpush ulist1 2]
	assert_equal 7 [r lpush ulist1 3]
	assert_equal 8 [r lpush ulist1 5]
	assert_equal 9 [r lpush ulist1 6]
	assert_equal 10 [r lpush ulist1 7]
	assert_equal 11 [r lpush ulist1 8]
	assert_equal 12 [r lpush ulist1 9]
	assert_encoding ziplist ulist1
	assert_equal {9 8 7 6 5 3 2 1 4} [r llunique ulist1]
	assert_equal {9 8 7 6 5 4 3 2 1} [r lrunique ulist1]
	assert_equal 9 [r lluniquestore ull1 ulist1]
	assert_equal 9 [r lruniquestore ulr1 ulist1]
	assert_encoding linkedlist ull1
	assert_encoding linkedlist ulr1
	assert_equal {9 8 7 6 5 3 2 1 4} [r lrange ull1 0 -1]
	assert_equal {9 8 7 6 5 4 3 2 1} [r lrange ulr1 0 -1]
	assert_equal 13 [r lpush ulist1 $largevalue(linkedlist)]
	assert_equal 14 [r lpush ulist1 aaa]
	assert_encoding linkedlist ulist1
	assert_equal "aaa $largevalue(linkedlist) 9 8 7 6 5 3 2 1 4" [r llunique ulist1]
	assert_equal "aaa $largevalue(linkedlist) 9 8 7 6 5 4 3 2 1" [r lrunique ulist1]
	assert_equal 11 [r lluniquestore ull1 ulist1]
	assert_equal 11 [r lruniquestore ulr1 ulist1]
	assert_equal "aaa $largevalue(linkedlist) 9 8 7 6 5 3 2 1 4" [r lrange ull1 0 -1]
	assert_equal "aaa $largevalue(linkedlist) 9 8 7 6 5 4 3 2 1" [r lrange ulr1 0 -1]
	assert_equal 1 [r del ulr1]
	assert_equal 1 [r del ull1]
	assert_equal 1 [r del ulist1]
    }

    test {LFOREACHSSTORE} {
	assert_equal 1 [r rpush lidx 1]
	assert_equal 2 [r rpush lidx 2]
	assert_equal 3 [r rpush lidx 3]
	assert_equal 4 [r rpush lidx 4]
	assert_equal 5 [r rpush lidx 5]
	assert_equal 1 [r sadd set1val 1]
	assert_equal 1 [r sadd set1val 2]
	assert_equal 1 [r sadd set1val 3]
	assert_equal 1 [r sadd set2val a3]
	assert_equal 1 [r sadd set2val a2]
	assert_equal 1 [r sadd set2val a1]
	assert_equal 1 [r sadd set3val set3a1]
	assert_equal 1 [r sadd set4val set4a1]
	assert_equal 1 [r sadd set5val set5a1]
	assert_equal 1 [r sadd set5val 1]
	assert_equal 1 [r sadd set5val 2]
	assert_equal 1 [r sadd set5val 3]
	assert_equal 1 [r sadd set5val 4]
	assert_equal 1 [r sadd set5val 5]
	assert_equal 14 [r lforeachsstore ldst lidx set*val]
    }

    test {L2SSTORE - ziplist} {
	assert_equal 1 [r lpush myziplist3 a]
        assert_equal 2 [r rpush myziplist3 b]
        assert_equal 3 [r rpush myziplist3 c]
        assert_equal 4 [r lpush myziplist3 d]
        assert_equal 5 [r lpush myziplist3 c]
	assert_equal 5 [r llen  myziplist3]
        assert_encoding ziplist myziplist3
        assert_equal 4 [r l2Sstore myset myziplist3]
        assert_equal 4 [r scard myset]
    }

    test {L2SSTORE - regular list} {
        assert_equal 1 [r lpush mylist3 $largevalue(linkedlist)]
        assert_encoding linkedlist mylist3
        assert_equal 2 [r rpush mylist3 b]
        assert_equal 3 [r rpush mylist3 c]
        assert_equal 4 [r lpush mylist3 d]
        assert_equal 5 [r lpush mylist3 c]
	assert_equal 5 [r llen  mylist3]
        assert_equal 4 [r l2sstore myset2 mylist3]
        assert_equal 4 [r scard myset2]
    }

    test "ZRANKORNEXT" {
	assert_equal 1 [r zadd zrtmp 0 1]
	assert_equal 1 [r zadd zrtmp 0 3]
	assert_equal 1 [r zadd zrtmp 0 a]
	assert_equal 1 [r zadd zrtmp 0 4]
	assert_equal 1 [r zadd zrtmp 0 2]
	assert_equal 0 [r zrankornext zrtmp 1]
	assert_equal 1 [r zrankornext zrtmp 2]
	assert_equal 2 [r zrankornext zrtmp 3]
	assert_equal 3 [r zrankornext zrtmp 4]
	assert_equal 4 [r zrankornext zrtmp a]
	assert_equal 5 [r zrankornext zrtmp b]
	assert_equal 0 [r zrankornext zrtmp 0]
	assert_equal 1 [r zrankornext zrtmp 11]
	assert_equal 5 [r zrankornext zrtmp aaa]
	assert_equal 4 [r zrankornext zrtmp 9a9]
	assert_equal 2 [r zrankornext zrtmp 222]
    }

    test "ZREVRANKORNEXT" {
	# zrtmp { 1 2 3 4 a } [0-4]
        #       5 4 3 2 1 0
	assert_equal 4 [r zrevrankornext zrtmp 1]
	assert_equal 3 [r zrevrankornext zrtmp 2]
	assert_equal 2 [r zrevrankornext zrtmp 3]
	assert_equal 1 [r zrevrankornext zrtmp 4]
	assert_equal 0 [r zrevrankornext zrtmp a]
	assert_equal 0 [r zrevrankornext zrtmp b]
	assert_equal 5 [r zrevrankornext zrtmp 0]
	assert_equal 4 [r zrevrankornext zrtmp 11]
	assert_equal 0 [r zrevrankornext zrtmp aaa]
	assert_equal 1 [r zrevrankornext zrtmp 9a9]
	assert_equal 3 [r zrevrankornext zrtmp 222]	
    }

    test "ZRANGEBYSCORENMEMBER" {
	# 0:{1 2 3 4 a} 1:{a1 a2 a3 a4}
	assert_equal 1 [r zadd zrtmp 1 a1]
	assert_equal 1 [r zadd zrtmp 1 a2]
	assert_equal 1 [r zadd zrtmp 1 a3]
	assert_equal 1 [r zadd zrtmp 1 a4]
	assert_equal {1 2 3 4} [r zrangebyscorenmember zrtmp 0 0 1 4]
	assert_equal {a1 a2 a3 a4} [r zrangebyscorenmember zrtmp 1 1 a f]
	assert_equal {a a1 a2 a3 a4} [r zrangebyscorenmember zrtmp 0 1 a f]
	assert_equal {1 2 3 4 a a1 a2 a3 a4} [r zrangebyscorenmember zrtmp 0 1 0 f]
	assert_equal 1 [r zadd zrtmp 0.5 ff]
	assert_equal {2 3 4 a ff} [r zrangebyscorenmember zrtmp 0 (1 2 f]
	assert_equal {ff a1 a2 a3 a4} [r zrangebyscorenmember zrtmp (0 1 0 f]
	assert_equal {ff} [r zrangebyscorenmember zrtmp (0 (1 0 f]
    }

}
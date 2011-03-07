start_server {
    tags {"meshin"}
    overrides {
        "list-max-ziplist-value" 16
        "list-max-ziplist-entries" 256
    }
} {
    test {GROUPSUM wrong number of arguments, low} {
	catch {r groupsum sumlist keylist keys* } err
	format $err
    } {ERR*}

    test {GROUPSORT wrong number of arguments, low} {
	catch {r groupsort sortlist keylist keys* 0 -1} err
	format $err
    } {ERR*}

    test {GROUPSORT wrong number of arguments, high} {
	catch {r groupsort sortlist keylist keys* sortp* 0 -1 DESC ALPHA HIGH} err
	format $err
    } {ERR*}

    test {GROUPSORT wrong arguments} {
	catch {r groupsort sortlist keylist keys* sortp* 0 -1 DESC ALPH} err
	format $err
    } {ERR*}

    test {GROUPSORT wrong arguments} {
	catch {r groupsort sortlist keylist keys* sortp* 0 -1 ALPH} err
	format $err
    } {ERR*}

    test {GROUPSORT wrong arguments} {
	catch {r groupsort sortlist keylist keys* sortp* 0 -1 ALPHA DESC} err
	format $err
    } {ERR*}

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
	assert_equal 1 [r hset hp s1 1]
	assert_equal 1 [r hset hp s2 10]
	assert_equal 1 [r hset hp s3 100]
	assert_equal 1 [r hset hp ss1 2]
	assert_equal 1 [r hset hp ss2 20]
	assert_equal 1 [r hset hp ss3 200]
	assert_equal 1 [r hset hp sss1 3]
	assert_equal 1 [r hset hp sss2 30]
	assert_equal 1 [r hset hp sss3 300]
	assert_equal 9 [r groupsum sumlist keylist keys* hp->s* hp->ss* hp->sss*]
	assert_equal {111 222 333 100 200 300 11 22 33} [r lrange sumlist 0 -1]
    }

    test {GROUPSORT} {
	assert_equal OK [r set sby1 9]
	assert_equal OK [r set sby2 8]
	assert_equal OK [r set sby3 7]
	assert_equal 6 [r groupsort sortlist keylist keys* sby* 0 -1 ASC]
	assert_equal {3 2 1 3 2 1} [r lrange sortlist 0 -1]
	assert_equal 6 [r groupsort sortlist keylist keys* sby* 0 -1 DESC]
	assert_equal {1 2 3 3 1 2} [r lrange sortlist 0 -1]
	assert_equal OK [r set sby1 aaa]
	assert_equal OK [r set sby2 a]
	assert_equal OK [r set sby3 b]
	assert_equal 6 [r groupsort sortlist keylist keys* sby* 0 -1 ASC]
	assert_equal {1 2 3 3 1 2} [r lrange sortlist 0 -1]
	assert_equal 6 [r groupsort sortlist keylist keys* sby* 0 -1 ASC ALPHA]
	assert_equal {2 1 3 3 2 1} [r lrange sortlist 0 -1]
	assert_equal 1 [r hset hby k1 aaa]
	assert_equal 1 [r hset hby k2 a]
	assert_equal 1 [r hset hby k3 b]
	assert_equal 6 [r groupsort sortlist keylist keys* hby->k* 0 -1 ASC ALPHA]
	assert_equal {2 1 3 3 2 1} [r lrange sortlist 0 -1]
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
	test {LFOREACHSSTORE hset} {
		assert_equal 1 [r hset hs 1 a1]
		assert_equal 1 [r hset hs 2 a2]
		assert_equal 1 [r hset hs 3 a3]
		assert_equal 1 [r hset hs 4 a5]
		assert_equal 1 [r hset hs 5 a5]
		assert_equal 0 [r lforeachsstore ldst lidx hset->*]
    }

    test {LFOREACHSTORE wrong arguments} {
		catch {r lforeachsstore ldst set1val set* } err
		format $err
    } {ERR*}

    test {LFOREACHSTORE wrong # arguments} {
		catch {r lforeachsstore ldst set1val } err
		format $err
    } {ERR*}

    test {LFOREACHSTORE wrong # arguments} {
		catch {r lforeachsstore ldst set1val set* sett* } err
		format $err
    } {ERR*}

	test {LFOREACHSTORE wrong patterns} {
		assert_equal {} [r lforeachsstore ldst lid set* ]
		assert_equal 0 [r lforeachsstore ldst lidx set* ]
	}

	test {SFOREACHSSTORE wrong # params} {
		catch {r sforeachsstore sdst set1val set* sett* } err
		format $err
    } {ERR*}

	test {SFOREACHSSTORE wrong # params} {
		catch {r sforeachsstore sdst set1val} err
		format $err
    } {ERR*}

	test {SFOREACHSSTORE} {
		assert_equal {} [r sforeachsstore sdst ldst keys* ]
		assert_equal 1 [r sadd sidx 1]
		assert_equal 1 [r sadd sidx 2]
		assert_equal 1 [r sadd sidx 3]
		assert_equal 1 [r sadd sidx 4]
		assert_equal 1 [r sadd sidx 5]
		assert_equal 0 [r sforeachsstore sdst sidx k* ]
		assert_equal 11 [r sforeachsstore sdst sidx set*val ]
    }

	test {SFOREACHSSTORE hset} {
		assert_equal 0 [r sforeachsstore sdst sidx hset->*]
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

    test {ZRANGESTORE wrong arguments} {
		catch {r zrangestore ldst set1val 0 -1 } err
		format $err
    } {ERR*}

	test "ZRANGESTORE" {
		assert_equal 5 [r zrangestore ldst zrtmp 0 -1]
		assert_equal {1 2 3 4 a} [r lrange ldst 0 -1]
		assert_equal 4 [r zrangestore ldst zrtmp 1 -1]
		assert_equal {2 3 4 a} [r lrange ldst 0 -1]
		assert_equal 4 [r zrangestore ldst zrtmp 0 3]
		assert_equal {1 2 3 4} [r lrange ldst 0 -1]
	}

    test {ZREVRANGESTORE wrong arguments} {
		catch {r zrevrangestore ldst set1val 0 -1 } err
		format $err
    } {ERR*}

	test "ZREVRANGESTORE" {
		assert_equal 5 [r zrevrangestore ldst zrtmp 0 -1]
		assert_equal {a 4 3 2 1} [r lrange ldst 0 -1]
		assert_equal 4 [r zrevrangestore ldst zrtmp 1 -1]
		assert_equal {4 3 2 1} [r lrange ldst 0 -1]
		assert_equal 4 [r zrevrangestore ldst zrtmp 0 3]
		assert_equal {a 4 3 2} [r lrange ldst 0 -1]
	}

    test {ZRANGEBYSCORESTORE wrong arguments} {
		catch {r zrangebyscorestore ldst set1val 0 -1 } err
		format $err
    } {ERR*}

	test "ZRANGEBYSCORESTORE" {
		assert_equal {} [r zrangebyscorestore ldst zrtmp 1 1]
		assert_equal 5 [r zrangebyscorestore ldst zrtmp 0 1]
	}

    test {ZREVRANGEBYSCORESTORE wrong arguments} {
		catch {r zrevrangebyscorestore ldst set1val 0 -1 } err
		format $err
    } {ERR*}

	test "ZREVRANGEBYSCORESTORE" {
		assert_equal {} [r zrangebyscorestore ldst zrtmp 1 1]
		assert_equal 5 [r zrangebyscorestore ldst zrtmp 0 1]
	}

    test {ZRANGEBYSCORENMEMBER wrong # of args} {
		catch {r zrangebyscorenmember zrtmp 0 0 0 0 0} err
		format $err
    } {ERR*}

    test {ZRANGEBYSCORENMEMBER wrong # of args} {
		catch {r zrangebyscorenmember zrtmp 0 0 0 } err
		format $err
    } {ERR*}

    test {ZRANGEBYSCORENMEMBER wrong type of args} {
		catch {r zrangebyscorenmember set1val 0 1 0 f} err
		format $err
    } {ERR*}

    test {ZRANGEBYSCORENMEMBER wrong type of args} {
		catch {r zrangebyscorenmember zrtmp a b 0 f} err
		format $err
    } {ERR*}

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

    test {ZRANGEBYSCORENMEMBERSTORE wrong # of args} {
		catch {r zrangebyscorenmemberstore ldst zrtmp 0 0 0 0 0} err
		format $err
    } {ERR*}

    test {ZRANGEBYSCORENMEMBERSTORE wrong # of args} {
		catch {r zrangebyscorenmemberstore ldst zrtmp 0 0 0 } err
		format $err
    } {ERR*}

    test {ZRANGEBYSCORENMEMBERSTORE wrong type of args} {
		catch {r zrangebyscorenmemberstore ldst set1val 0 0 0 0 } err
		format $err
    } {ERR*}

    test {ZRANGEBYSCORENMEMBERSTORE wrong type of args} {
		catch {r zrangebyscorenmemberstore ldst zrtmp a b 0 f} err
		format $err
    } {ERR*}

    test "ZRANGEBYSCORENMEMBERSTORE" {
		assert_equal 4 [r zrangebyscorenmemberstore ldst zrtmp 0 0 1 4]
		assert_equal {1 2 3 4} [r lrange ldst 0 -1]
		assert_equal 4 [r zrangebyscorenmemberstore ldst zrtmp 1 1 a f]
		assert_equal {a1 a2 a3 a4} [r lrange ldst 0 -1]
		assert_equal 10 [r zrangebyscorenmemberstore ldst zrtmp 0 1 0 f]
		assert_equal {1 2 3 4 a ff a1 a2 a3 a4} [r lrange ldst 0 -1]
		assert_equal 5 [r zrangebyscorenmemberstore ldst zrtmp 0 (1 2 f]
		assert_equal {2 3 4 a ff} [r lrange ldst 0 -1]
		assert_equal 5 [r zrangebyscorenmemberstore ldst zrtmp (0 1 0 f]
		assert_equal {ff a1 a2 a3 a4} [r lrange ldst 0 -1]
		assert_equal 1 [r zrangebyscorenmemberstore ldst zrtmp (0 (1 0 f]
		assert_equal {ff} [r lrange ldst 0 -1]
    }
}

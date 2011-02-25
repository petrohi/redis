#include "redis.h"
#include "pqsort.h" /* Partial qsort for SORT+LIMIT */

/*
  types of pattern:
   1. "#" or "" -> direct subst with a subst key
   2. "'string' with '*'" -> subst '*' with a key and look it up
   3. "'string' with '->'" -> add a key and look ip up in a hash 'string'

   p1 == p1 == -1 -> 1.
   p1 >=0         -> 2. p1 - location of a '*'
   p2 >=0         -> 3. p2 - first char after '->'

   could be either
   1. prefix*postfix->field
   2. keyname->prefix*postfix

        0     !-p1    !-p2   !-plen
    1. 'prefix*postfix->field' 
        0      !-p2    !-p1    !-peln
    2. 'keyname->prefix*postfix'
        0  !-p1
    3. 'key*name'   p2=-1
   
      p1 - position of '*'
      p2 - position of '->'  - could be -1 - no hash
*/

int initPattern(redisPattern *p, robj *str) {
    p->p1=-1;
    p->p2=-1;
    p->pt=getDecodedObject(str);
    sds spat=p->pt->ptr;
    if (spat[0]=='#' && spat[1] == '\0') {
	return REDIS_OK;
    }
    char *ss=strchr(spat,'*');
    if (!ss) {
	p->p2=0;
	return REDIS_ERR;
    }
    p->p1=ss-spat;
    
    char *ha=strstr(spat,"->");
    if (ha) {
	p->p2=ha-spat;
    }
    redisLog(REDIS_DEBUG, "pattern init '%s' [%d,%d]", p->pt->ptr, p->p1, p->p2);
    return REDIS_OK;
}

void releasePattern(redisPattern *p) {
    if (p->pt) {
	decrRefCount(p->pt);
	p->pt=NULL;
    }
    p->p1=-1;
    p->p2=-1;
}

redisSortOperation *createSortOperation(int type, robj *pattern) {
    redisSortOperation *so = zmalloc(sizeof(*so));
    so->type = type;
    initPattern(&(so->pattern), pattern);
    return so;
}

void releaseSortOperation(void *ptr) {
    if (ptr) {
	releasePattern(&(((redisSortOperation*)ptr)->pattern));
	zfree(ptr);
    }
}

robj *lookupKeyByPatternS(redisDb *db, redisPattern *p, robj *subst) {
    if (p->p1==-1 && p->p2==-1) {
        incrRefCount(subst);
        return subst;
    }
    if (p->p1==-1 || p->pt==NULL) /* there is no '*' in the p */
	return NULL;

    subst = getDecodedObject(subst);    

    struct {
        int len;
        int free;
        char buf[REDIS_SORTKEY_MAX+1];
    } keyname, fieldname;

    int plen = sdslen(p->pt->ptr);
    int slen = sdslen(subst->ptr);

    char *pS = p->pt->ptr; // source
    char *pD = keyname.buf; // dst

    int prefix=p->p1;
    int postfix=plen-p->p1-1;

    if (p->p2==-1) { /* no '->' */
	fieldname.len=0;
	keyname.len=plen+slen-1;
    }
    else {
	if (p->p2>=0 && p->p2<p->p1) {
	    keyname.len = p->p2;
	    memcpy(keyname.buf,((char*)(p->pt->ptr)),keyname.len);
	    pD = fieldname.buf;
	    pS += p->p2+2;
	    prefix -= p->p2+2;
	    fieldname.len = postfix + prefix + slen;
	}
	else {
	    postfix=p->p2-p->p1-1;
	    fieldname.len = plen-p->p2-2;
	    memcpy(fieldname.buf,((char*)(p->pt->ptr))+p->p2+2,fieldname.len+1); /* copy '\0' */
	    keyname.len = postfix + prefix + slen;
	}
    }

    memcpy(pD, pS, prefix);
    memcpy(pD+prefix,subst->ptr,slen);
    memcpy(pD+prefix+slen, pS+1+prefix, postfix);
    keyname.buf[keyname.len] = '\0';

    robj keyobj;

    /* Lookup substituted key */
    initStaticStringObject(keyobj,((char*)&keyname)+(sizeof(struct sdshdr)));
    robj *o = lookupKeyRead(db,&keyobj);

    if (fieldname.len > 0) {
	redisLog(REDIS_DEBUG, " lookup ('%s'[%d,%d])('%s') -> '%s->%s'", p->pt->ptr, p->p1, p->p2, subst->ptr, keyname.buf, fieldname.buf);
    }
    else {
	redisLog(REDIS_DEBUG, " lookup ('%s'[%d,%d])('%s') -> '%s'", p->pt->ptr, p->p1, p->p2, subst->ptr, keyname.buf);
    }

    decrRefCount(subst);

    if (o == NULL) return NULL;

    if (fieldname.len > 0) {
        if (o->type != REDIS_HASH) return NULL;
        /* Retrieve value from hash by the field name. This operation
         * already increases the refcount of the returned object. */
	robj fieldobj;
        initStaticStringObject(fieldobj,((char*)&fieldname)+(sizeof(struct sdshdr)));
        o = hashTypeGetObject(o, &fieldobj);
    } else {
        /* Every object that this function returns needs to have its refcount
         * increased. sortCommand decreases it again. */
        incrRefCount(o);
    }
    return o;
}

#if 0
/* Return the value associated to the key with a name obtained
 * substituting the first occurence of '*' in 'pattern' with 'subst'.
 * The returned object will always have its refcount increased by 1
 * when it is non-NULL. */
robj *lookupKeyByPattern(redisDb *db, robj *pattern, robj *subst) {
    char *p, *f;
    sds spat, ssub;
    robj keyobj, fieldobj, *o;
    int prefixlen, sublen, postfixlen, fieldlen;
    /* Expoit the internal sds representation to create a sds string allocated on the stack in order to make this function faster */
    struct {
        int len;
        int free;
        char buf[REDIS_SORTKEY_MAX+1];
    } keyname, fieldname;

    /* If the pattern is "#" return the substitution object itself in order
     * to implement the "SORT ... GET #" feature. */
    spat = pattern->ptr;
    if (spat[0] == '#' && spat[1] == '\0') {
        incrRefCount(subst);
        return subst;
    }

    /* The substitution object may be specially encoded. If so we create
     * a decoded object on the fly. Otherwise getDecodedObject will just
     * increment the ref count, that we'll decrement later. */
    subst = getDecodedObject(subst);

    ssub = subst->ptr;
    if (sdslen(spat)+sdslen(ssub)-1 > REDIS_SORTKEY_MAX) return NULL;
    p = strchr(spat,'*');
    if (!p) {
        decrRefCount(subst);
        return NULL;
    }

    /* Find out if we're dealing with a hash dereference. */
    if ((f = strstr(p+1, "->")) != NULL) {
        fieldlen = sdslen(spat)-(f-spat);
        /* this also copies \0 character */
        memcpy(fieldname.buf,f+2,fieldlen-1);
        fieldname.len = fieldlen-2;
    } else {
        fieldlen = 0;
    }

    prefixlen = p-spat;
    sublen = sdslen(ssub);
    postfixlen = sdslen(spat)-(prefixlen+1)-fieldlen;
    memcpy(keyname.buf,spat,prefixlen);
    memcpy(keyname.buf+prefixlen,ssub,sublen);
    memcpy(keyname.buf+prefixlen+sublen,p+1,postfixlen);
    keyname.buf[prefixlen+sublen+postfixlen] = '\0';
    keyname.len = prefixlen+sublen+postfixlen;
    decrRefCount(subst);

    /* Lookup substituted key */
    initStaticStringObject(keyobj,((char*)&keyname)+(sizeof(struct sdshdr)));
    o = lookupKeyRead(db,&keyobj);
    if (o == NULL) return NULL;

    if (fieldlen > 0) {
        if (o->type != REDIS_HASH || fieldname.len < 1) return NULL;

        /* Retrieve value from hash by the field name. This operation
         * already increases the refcount of the returned object. */
        initStaticStringObject(fieldobj,((char*)&fieldname)+(sizeof(struct sdshdr)));
        o = hashTypeGetObject(o, &fieldobj);
    } else {
        if (o->type != REDIS_STRING) return NULL;

        /* Every object that this function returns needs to have its refcount
         * increased. sortCommand decreases it again. */
        incrRefCount(o);
    }

    return o;
}
#endif

/* sortCompare() is used by qsort in sortCommand(). Given that qsort_r with
 * the additional parameter is not standard but a BSD-specific we have to
 * pass sorting parameters via the global 'server' structure */
int sortCompare(const void *s1, const void *s2) {
    const redisSortObject *so1 = s1, *so2 = s2;
    int cmp;

    if (!server.sort_alpha) {
        /* Numeric sorting. Here it's trivial as we precomputed scores */
        if (so1->u.score > so2->u.score) {
            cmp = 1;
        } else if (so1->u.score < so2->u.score) {
            cmp = -1;
        } else {
            cmp = 0;
        }
    } else {
        /* Alphanumeric sorting */
        if (server.sort_bypattern) {
            if (!so1->u.cmpobj || !so2->u.cmpobj) {
                /* At least one compare object is NULL */
                if (so1->u.cmpobj == so2->u.cmpobj)
                    cmp = 0;
                else if (so1->u.cmpobj == NULL)
                    cmp = -1;
                else
                    cmp = 1;
            } else {
                /* We have both the objects, use strcoll */
                cmp = strcoll(so1->u.cmpobj->ptr,so2->u.cmpobj->ptr);
            }
        } else {
            /* Compare elements directly. */
            cmp = compareStringObjects(so1->obj,so2->obj);
        }
    }
    return server.sort_desc ? -cmp : cmp;
}

/* SORT: actual sorting */
redisSortObject* sortVectorEx(redisClient *c, robj *sortval,
			      int desc, int alpha, int *lstart, int *lcount,
			      int dontsort, redisPattern* sortby, int *lvector)
{
    int vectorlen;

    /* Load the sorting vector with all the objects to sort */
    switch(sortval->type) {
    case REDIS_LIST: vectorlen = listTypeLength(sortval); break;
    case REDIS_SET: vectorlen =  setTypeSize(sortval); break;
    case REDIS_ZSET: vectorlen = dictSize(((zset*)sortval->ptr)->dict); break;
    default: vectorlen = 0; redisPanic("Bad SORT type"); /* Avoid GCC warning */
    }

    /* Resulting vector to sort */
    redisSortObject *vector = zmalloc(sizeof(redisSortObject)*vectorlen);
    *lvector = vectorlen;

    int j = 0;
    if (sortval->type == REDIS_LIST) {
        listTypeIterator *li = listTypeInitIterator(sortval,0,REDIS_TAIL);
        listTypeEntry entry;
        while(listTypeNext(li,&entry)) {
            vector[j].obj = listTypeGet(&entry);
            vector[j].u.score = 0;
            vector[j].u.cmpobj = NULL;
            j++;
        }
        listTypeReleaseIterator(li);
    } else if (sortval->type == REDIS_SET) {
        setTypeIterator *si = setTypeInitIterator(sortval);
        robj *ele;
        while((ele = setTypeNextObject(si)) != NULL) {
            vector[j].obj = ele;
            vector[j].u.score = 0;
            vector[j].u.cmpobj = NULL;
            j++;
        }
        setTypeReleaseIterator(si);
    } else if (sortval->type == REDIS_ZSET) {
        dict *set = ((zset*)sortval->ptr)->dict;
        dictIterator *di;
        dictEntry *setele;
        di = dictGetIterator(set);
        while((setele = dictNext(di)) != NULL) {
            vector[j].obj = dictGetEntryKey(setele);
            vector[j].u.score = 0;
            vector[j].u.cmpobj = NULL;
            j++;
        }
        dictReleaseIterator(di);
    } else {
        redisPanic("Unknown type");
    }
    redisAssert(j == vectorlen);

    /* Now it's time to load the right scores in the sorting vector */
    if (dontsort == 0) {
        for (j = 0; j < vectorlen; j++) {
            robj *byval;
            if (sortby->pt) {
                /* lookup value to sort by */
                byval = lookupKeyByPatternS(c->db,sortby,vector[j].obj);
                if (!byval) continue;
            } else {
                /* use object itself to sort by */
                byval = vector[j].obj;
            }

            if (alpha) {
                if (sortby->pt) vector[j].u.cmpobj = getDecodedObject(byval);
            } else {
                if (byval->encoding == REDIS_ENCODING_RAW) {
                    vector[j].u.score = strtod(byval->ptr,NULL);
                } else if (byval->encoding == REDIS_ENCODING_INT) {
                    /* Don't need to decode the object if it's
                     * integer-encoded (the only encoding supported) so
                     * far. We can just cast it */
                    vector[j].u.score = (long)byval->ptr;
                } else {
                    redisAssert(1 != 1);
                }
            }

            /* when the object was retrieved using lookupKeyByPattern,
             * its refcount needs to be decreased. */
            if (sortby->pt) {
                decrRefCount(byval);
            }
        }
    }
    /* We are ready to sort the vector... perform a bit of sanity check
     * on the LIMIT option too. We'll use a partial version of quicksort. */
    int start = ((*lstart) < 0) ? 0 : (*lstart);
    int end = ((*lcount) < 0) ? vectorlen-1 : start+(*lcount)-1;

    if (start >= vectorlen) {
        start = vectorlen-1;
        end = vectorlen-2;
    }
    if (end >= vectorlen) end = vectorlen-1;

    if (dontsort == 0) {
        server.sort_desc = desc;
        server.sort_alpha = alpha;
        server.sort_bypattern = sortby->pt ? 1 : 0;
        if (sortby->pt && (start != 0 || end != vectorlen-1))
            pqsort(vector,vectorlen,sizeof(redisSortObject),sortCompare,start,end);
        else
            qsort(vector,vectorlen,sizeof(redisSortObject),sortCompare);
    }
    /* return adjusted limits */
    *lstart=start;
    *lcount=end;

    return vector;
}

/* The SORT command is the most complex command in Redis. Warning: this code
 * is optimized for speed and a bit less for readability */
void sortCommand(redisClient *c) {
    list *operations;
    int desc = 0, alpha = 0;
    int limit_start = 0, limit_count = -1;
    int dontsort = 0, vectorlen=0;
    int getop = 0; /* GET operation counter */
    robj *sortval, *storekey = NULL;
    redisPattern sortby; sortby.pt=NULL;

    /* Lookup the key to sort. It must be of the right types */
    sortval = lookupKeyRead(c->db,c->argv[1]);
    if (sortval == NULL) {
        addReply(c,shared.emptymultibulk);
        return;
    }
    if (sortval->type != REDIS_SET && sortval->type != REDIS_LIST &&
        sortval->type != REDIS_ZSET)
    {
        addReply(c,shared.wrongtypeerr);
        return;
    }

    /* Create a list of operations to perform for every sorted element.
     * Operations can be GET/DEL/INCR/DECR */
    operations = listCreate();
    listSetFreeMethod(operations,releaseSortOperation);
    int j = 2;

    /* Now we need to protect sortval incrementing its count, in the future
     * SORT may have options able to overwrite/delete keys during the sorting
     * and the sorted key itself may get destroied */
    incrRefCount(sortval);

    /* The SORT command has an SQL-alike syntax, parse it */
    while(j < c->argc) {
        int leftargs = c->argc-j-1;
        if (!strcasecmp(c->argv[j]->ptr,"asc")) {
            desc = 0;
        } else if (!strcasecmp(c->argv[j]->ptr,"desc")) {
            desc = 1;
        } else if (!strcasecmp(c->argv[j]->ptr,"alpha")) {
            alpha = 1;
        } else if (!strcasecmp(c->argv[j]->ptr,"limit") && leftargs >= 2) {
            limit_start = atoi(c->argv[j+1]->ptr);
            limit_count = atoi(c->argv[j+2]->ptr);
            j+=2;
        } else if (!strcasecmp(c->argv[j]->ptr,"store") && leftargs >= 1) {
            storekey = c->argv[j+1];
            j++;
        } else if (!strcasecmp(c->argv[j]->ptr,"by") && leftargs >= 1) {
            initPattern(&sortby,c->argv[j+1]); //sortby = c->argv[j+1];
            /* If the BY pattern does not contain '*', i.e. it is constant,
             * we don't need to sort nor to lookup the weight keys. */
            if (strchr(c->argv[j+1]->ptr,'*') == NULL) dontsort = 1;
            j++;
        } else if (!strcasecmp(c->argv[j]->ptr,"get") && leftargs >= 1) {
            listAddNodeTail(operations,createSortOperation(
                REDIS_SORT_GET,c->argv[j+1]));
            getop++;
            j++;
        } else {
            decrRefCount(sortval);
            listRelease(operations);
            addReply(c,shared.syntaxerr);
	    releasePattern(&sortby);
            return;
        }
        j++;
    }

    int start = limit_start;
    int end = limit_count;

    /* sortVectorEx will afjust start,end */
    redisSortObject* vector = sortVectorEx(c, sortval, desc, alpha, &start, &end, dontsort, &sortby, &vectorlen);

    /* Send command output to the output buffer, performing the specified
     * GET/DEL/INCR/DECR operations if any. */
    int outputlen = getop ? getop*(end-start+1) : end-start+1;
    if (storekey == NULL) {
        /* STORE option not specified, sent the sorting result to client */
        addReplyMultiBulkLen(c,outputlen);
        for (j = start; j <= end; j++) {
            listNode *ln;
            listIter li;

            if (!getop) addReplyBulk(c,vector[j].obj);
            listRewind(operations,&li);
            while((ln = listNext(&li))) {
                redisSortOperation *sop = ln->value;
                // robj *val = lookupKeyByPattern(c->db,sop->pattern,vector[j].obj);
		robj *val = lookupKeyByPatternS(c->db,&(sop->pattern),vector[j].obj);

                if (sop->type == REDIS_SORT_GET) {
                    if (!val) {
                        addReply(c,shared.nullbulk);
                    } else {
                        addReplyBulk(c,val);
                        decrRefCount(val);
                    }
                } else {
                    redisAssert(sop->type == REDIS_SORT_GET); /* always fails */
                }
            }
        }
    } else {
        robj *sobj = createZiplistObject();

        /* STORE option specified, set the sorting result as a List object */
        for (j = start; j <= end; j++) {
            listNode *ln;
            listIter li;

            if (!getop) {
                listTypePush(sobj,vector[j].obj,REDIS_TAIL);
            } else {
                listRewind(operations,&li);
                while((ln = listNext(&li))) {
                    redisSortOperation *sop = ln->value;
                    robj *val = lookupKeyByPatternS(c->db,&(sop->pattern),
						    vector[j].obj);

                    if (sop->type == REDIS_SORT_GET) {
                        if (!val) val = createStringObject("",0);

                        /* listTypePush does an incrRefCount, so we should take care
                         * care of the incremented refcount caused by either
                         * lookupKeyByPattern or createStringObject("",0) */
                        listTypePush(sobj,val,REDIS_TAIL);
                        decrRefCount(val);
                    } else {
                        /* always fails */
                        redisAssert(sop->type == REDIS_SORT_GET);
                    }
                }
            }
        }
        dbReplace(c->db,storekey,sobj);
        /* Note: we add 1 because the DB is dirty anyway since even if the
         * SORT result is empty a new key is set and maybe the old content
         * replaced. */
        server.dirty += 1+outputlen;
        signalModifiedKey(c->db,storekey);
        addReplyLongLong(c,outputlen);
    }

    /* Cleanup */
    if (sortval->type == REDIS_LIST || sortval->type == REDIS_SET)
        for (j = 0; j < vectorlen; ++j)
            decrRefCount(vector[j].obj);
    decrRefCount(sortval);
    listRelease(operations);
    if (alpha) {
	for (j = 0; j < vectorlen; ++j) 
	    if (vector[j].u.cmpobj)
		decrRefCount(vector[j].u.cmpobj);
    }
    zfree(vector);
    releasePattern(&sortby);
}

#if 0
/* The SORT command is the most complex command in Redis. Warning: this code
 * is optimized for speed and a bit less for readability */
void sortCommand(redisClient *c) {
    list *operations;
    unsigned int outputlen = 0;
    int desc = 0, alpha = 0;
    int limit_start = 0, limit_count = -1, start, end;
    int j, dontsort = 0, vectorlen;
    int getop = 0; /* GET operation counter */
    robj *sortval, *sortby = NULL, *storekey = NULL;
    redisSortObject *vector; /* Resulting vector to sort */

    /* Lookup the key to sort. It must be of the right types */
    sortval = lookupKeyRead(c->db,c->argv[1]);
    if (sortval == NULL) {
        addReply(c,shared.emptymultibulk);
        return;
    }
    if (sortval->type != REDIS_SET && sortval->type != REDIS_LIST &&
        sortval->type != REDIS_ZSET)
    {
        addReply(c,shared.wrongtypeerr);
        return;
    }

    /* Create a list of operations to perform for every sorted element.
     * Operations can be GET/DEL/INCR/DECR */
    operations = listCreate();
    listSetFreeMethod(operations,zfree);
    j = 2;

    /* Now we need to protect sortval incrementing its count, in the future
     * SORT may have options able to overwrite/delete keys during the sorting
     * and the sorted key itself may get destroied */
    incrRefCount(sortval);

    /* The SORT command has an SQL-alike syntax, parse it */
    while(j < c->argc) {
        int leftargs = c->argc-j-1;
        if (!strcasecmp(c->argv[j]->ptr,"asc")) {
            desc = 0;
        } else if (!strcasecmp(c->argv[j]->ptr,"desc")) {
            desc = 1;
        } else if (!strcasecmp(c->argv[j]->ptr,"alpha")) {
            alpha = 1;
        } else if (!strcasecmp(c->argv[j]->ptr,"limit") && leftargs >= 2) {
            limit_start = atoi(c->argv[j+1]->ptr);
            limit_count = atoi(c->argv[j+2]->ptr);
            j+=2;
        } else if (!strcasecmp(c->argv[j]->ptr,"store") && leftargs >= 1) {
            storekey = c->argv[j+1];
            j++;
        } else if (!strcasecmp(c->argv[j]->ptr,"by") && leftargs >= 1) {
            sortby = c->argv[j+1];
            /* If the BY pattern does not contain '*', i.e. it is constant,
             * we don't need to sort nor to lookup the weight keys. */
            if (strchr(c->argv[j+1]->ptr,'*') == NULL) dontsort = 1;
            j++;
        } else if (!strcasecmp(c->argv[j]->ptr,"get") && leftargs >= 1) {
            listAddNodeTail(operations,createSortOperation(
                REDIS_SORT_GET,c->argv[j+1]));
            getop++;
            j++;
        } else {
            decrRefCount(sortval);
            listRelease(operations);
            addReply(c,shared.syntaxerr);
            return;
        }
        j++;
    }

    /* Load the sorting vector with all the objects to sort */
    switch(sortval->type) {
    case REDIS_LIST: vectorlen = listTypeLength(sortval); break;
    case REDIS_SET: vectorlen =  setTypeSize(sortval); break;
    case REDIS_ZSET: vectorlen = dictSize(((zset*)sortval->ptr)->dict); break;
    default: vectorlen = 0; redisPanic("Bad SORT type"); /* Avoid GCC warning */
    }
    vector = zmalloc(sizeof(redisSortObject)*vectorlen);
    j = 0;

    if (sortval->type == REDIS_LIST) {
        listTypeIterator *li = listTypeInitIterator(sortval,0,REDIS_TAIL);
        listTypeEntry entry;
        while(listTypeNext(li,&entry)) {
            vector[j].obj = listTypeGet(&entry);
            vector[j].u.score = 0;
            vector[j].u.cmpobj = NULL;
            j++;
        }
        listTypeReleaseIterator(li);
    } else if (sortval->type == REDIS_SET) {
        setTypeIterator *si = setTypeInitIterator(sortval);
        robj *ele;
        while((ele = setTypeNextObject(si)) != NULL) {
            vector[j].obj = ele;
            vector[j].u.score = 0;
            vector[j].u.cmpobj = NULL;
            j++;
        }
        setTypeReleaseIterator(si);
    } else if (sortval->type == REDIS_ZSET) {
        dict *set = ((zset*)sortval->ptr)->dict;
        dictIterator *di;
        dictEntry *setele;
        di = dictGetIterator(set);
        while((setele = dictNext(di)) != NULL) {
            vector[j].obj = dictGetEntryKey(setele);
            vector[j].u.score = 0;
            vector[j].u.cmpobj = NULL;
            j++;
        }
        dictReleaseIterator(di);
    } else {
        redisPanic("Unknown type");
    }
    redisAssert(j == vectorlen);

    /* Now it's time to load the right scores in the sorting vector */
    if (dontsort == 0) {
        for (j = 0; j < vectorlen; j++) {
            robj *byval;
            if (sortby) {
                /* lookup value to sort by */
                byval = lookupKeyByPattern(c->db,sortby,vector[j].obj);
                if (!byval) continue;
            } else {
                /* use object itself to sort by */
                byval = vector[j].obj;
            }

            if (alpha) {
                if (sortby) vector[j].u.cmpobj = getDecodedObject(byval);
            } else {
                if (byval->encoding == REDIS_ENCODING_RAW) {
                    vector[j].u.score = strtod(byval->ptr,NULL);
                } else if (byval->encoding == REDIS_ENCODING_INT) {
                    /* Don't need to decode the object if it's
                     * integer-encoded (the only encoding supported) so
                     * far. We can just cast it */
                    vector[j].u.score = (long)byval->ptr;
                } else {
                    redisAssert(1 != 1);
                }
            }

            /* when the object was retrieved using lookupKeyByPattern,
             * its refcount needs to be decreased. */
            if (sortby) {
                decrRefCount(byval);
            }
        }
    }

    /* We are ready to sort the vector... perform a bit of sanity check
     * on the LIMIT option too. We'll use a partial version of quicksort. */
    start = (limit_start < 0) ? 0 : limit_start;
    end = (limit_count < 0) ? vectorlen-1 : start+limit_count-1;
    if (start >= vectorlen) {
        start = vectorlen-1;
        end = vectorlen-2;
    }
    if (end >= vectorlen) end = vectorlen-1;

    if (dontsort == 0) {
        server.sort_desc = desc;
        server.sort_alpha = alpha;
        server.sort_bypattern = sortby ? 1 : 0;
        if (sortby && (start != 0 || end != vectorlen-1))
            pqsort(vector,vectorlen,sizeof(redisSortObject),sortCompare, start,end);
        else
            qsort(vector,vectorlen,sizeof(redisSortObject),sortCompare);
    }

    /* Send command output to the output buffer, performing the specified
     * GET/DEL/INCR/DECR operations if any. */
    outputlen = getop ? getop*(end-start+1) : end-start+1;
    if (storekey == NULL) {
        /* STORE option not specified, sent the sorting result to client */
        addReplyMultiBulkLen(c,outputlen);
        for (j = start; j <= end; j++) {
            listNode *ln;
            listIter li;

            if (!getop) addReplyBulk(c,vector[j].obj);
            listRewind(operations,&li);
            while((ln = listNext(&li))) {
                redisSortOperation *sop = ln->value;
                robj *val = lookupKeyByPattern(c->db,sop->pattern,
                    vector[j].obj);

                if (sop->type == REDIS_SORT_GET) {
                    if (!val) {
                        addReply(c,shared.nullbulk);
                    } else {
                        addReplyBulk(c,val);
                        decrRefCount(val);
                    }
                } else {
                    redisAssert(sop->type == REDIS_SORT_GET); /* always fails */
                }
            }
        }
    } else {
        robj *sobj = createZiplistObject();

        /* STORE option specified, set the sorting result as a List object */
        for (j = start; j <= end; j++) {
            listNode *ln;
            listIter li;

            if (!getop) {
                listTypePush(sobj,vector[j].obj,REDIS_TAIL);
            } else {
                listRewind(operations,&li);
                while((ln = listNext(&li))) {
                    redisSortOperation *sop = ln->value;
                    robj *val = lookupKeyByPattern(c->db,sop->pattern,
                        vector[j].obj);

                    if (sop->type == REDIS_SORT_GET) {
                        if (!val) val = createStringObject("",0);

                        /* listTypePush does an incrRefCount, so we should take care
                         * care of the incremented refcount caused by either
                         * lookupKeyByPattern or createStringObject("",0) */
                        listTypePush(sobj,val,REDIS_TAIL);
                        decrRefCount(val);
                    } else {
                        /* always fails */
                        redisAssert(sop->type == REDIS_SORT_GET);
                    }
                }
            }
        }
        dbReplace(c->db,storekey,sobj);
        /* Note: we add 1 because the DB is dirty anyway since even if the
         * SORT result is empty a new key is set and maybe the old content
         * replaced. */
        server.dirty += 1+outputlen;
        signalModifiedKey(c->db,storekey);
        addReplyLongLong(c,outputlen);
    }

    /* Cleanup */
    if (sortval->type == REDIS_LIST || sortval->type == REDIS_SET)
        for (j = 0; j < vectorlen; j++)
            decrRefCount(vector[j].obj);
    decrRefCount(sortval);
    listRelease(operations);
    for (j = 0; j < vectorlen; j++) {
        if (alpha && vector[j].u.cmpobj)
            decrRefCount(vector[j].u.cmpobj);
    }
    zfree(vector);
}
#endif

void groupsortstore(redisClient *c,
		    robj *dst, robj *key, robj *keypat, robj *sortpat,
		    int limit_start, int limit_count,
		    int desc, int alpha)
{
    robj *list, *dstlist, *obj;

    if ((list=lookupKeyReadOrReply(c,key,shared.nullbulk))==NULL ||
	checkType(c,list,REDIS_LIST) ||
	checkType(c,keypat,REDIS_STRING) || checkType(c,sortpat,REDIS_STRING))
	return;

    redisPattern pattern, sortby; pattern.pt=NULL; sortby.pt=NULL;
    int dontsort=0;
    if (strchr(sortpat->ptr,'*') == NULL) dontsort = 1;

    initPattern(&pattern, keypat);
    initPattern(&sortby, sortpat);

    dstlist=createZiplistObject(); /* let's start with zip list */

    listTypeIterator *li=listTypeInitIterator(list,0,REDIS_TAIL);
    listTypeEntry entry;

    while (listTypeNext(li,&entry)) {
	obj=listTypeGet(&entry);
	robj *sobj=lookupKeyByPatternS(c->db, &pattern, obj);
	if (sobj) {
	    if (sobj->type==REDIS_SET ||
		sobj->type==REDIS_ZSET || sobj->type==REDIS_LIST) {

		/* SORT sobj BY sortpat LIMIT 0 limit (start,count) GET # alpha desc */
		int start = limit_start;
		int end = limit_count;
		int vectorlen = 0;
		redisSortObject* vector=sortVectorEx(c, sobj, desc, alpha, &start, &end, dontsort, &sortby, &vectorlen);
		for (int j = start; j <= end; ++j) {
		    listTypePush(dstlist,vector[j].obj,REDIS_TAIL);
		}
		/* release redisSortObject */
		if (sobj->type==REDIS_LIST || sobj->type==REDIS_SET) {
		    for (int j = 0; j<vectorlen; ++j)
			decrRefCount(vector[j].obj);
		}
		if (alpha) {
		    for (int j = 0; j<vectorlen; ++j) 
			if (vector[j].u.cmpobj)
			    decrRefCount(vector[j].u.cmpobj);
		}
		zfree(vector);
	    }
	    decrRefCount(sobj);
	}
	decrRefCount(obj);
    }
    listTypeReleaseIterator(li);
    releasePattern(&sortby);
    releasePattern(&pattern);

    dbDelete(c->db, dst);
    
    if (listTypeLength(dstlist) > 0) {
	dbAdd(c->db,dst,dstlist);
	addReplyLongLong(c,listTypeLength(dstlist));
    } else {
	decrRefCount(dstlist);
	addReply(c,shared.czero);
    }

    signalModifiedKey(c->db,dst);
    server.dirty++;
}

/* GROUPSORT dst-list key-list key-pattern sort-pattern limit_min limit_count [ASC|DESC] [ALPHA] */
void groupsortCommand(redisClient *c) {
    int desc=0, alpha=0;
    int limit_start = atoi(c->argv[5]->ptr);
    int limit_count = atoi(c->argv[6]->ptr);
    if (c->argc>6 && !strcasecmp(c->argv[7]->ptr,"desc"))
	desc=1;
    if (c->argc>6 && !strcasecmp(c->argv[c->argc-1]->ptr,"alpha"))
	alpha=1;
    groupsortstore(c, c->argv[1], c->argv[2], c->argv[3], c->argv[4], limit_start, limit_count, desc, alpha);
}
/*  0          1        2         3          4  */
/* GROUPSUM dst-list key-list key-pattern pattern1 pattern2 ... patternN */
void groupsumCommand(redisClient *c) {
    robj *src;
    if ((src=lookupKeyReadOrReply(c,c->argv[2],shared.nullbulk))==NULL ||
	checkType(c,src,REDIS_LIST) || checkType(c,c->argv[3],REDIS_STRING))
	return;

    robj *dst=createZiplistObject();

    redisPattern pattern;
    initPattern(&pattern, c->argv[3]);

    int nptn = c->argc - 4;

    redisAssert(nptn > 0);

    redisPattern *ptn = zmalloc(sizeof(redisPattern)*nptn);

    for (int i=0; i<nptn; ++i)
	initPattern(&(ptn[i]), c->argv[4+i]);

    listTypeIterator *li=listTypeInitIterator(src,0,REDIS_TAIL);
    listTypeEntry entry;
    setTypeIterator *it;
    robj *obj, *sobj, *tmp;

    while (listTypeNext(li,&entry)) {
	obj=listTypeGet(&entry);
	sobj=lookupKeyByPatternS(c->db, &pattern, obj);
	if (sobj) {
	    switch (sobj->type) {
	    case REDIS_SET : {
		it=setTypeInitIterator(sobj);
		while ((tmp=setTypeNextObject(it))!=NULL) {
		    long long accu=0; /* int64_t? */
		    for (int i=0; i<nptn; ++i) {
			robj *sumobj=lookupKeyByPatternS(c->db, &(ptn[i]), tmp);
			if (sumobj) {
			    if (sumobj->type == REDIS_STRING) {
				long long value;
				if (getLongLongFromObject(sumobj, &value)==REDIS_OK)
				    accu+=value;
			    }
			    decrRefCount(sumobj);
			}
		    }
		    decrRefCount(tmp);
		    tmp=createStringObjectFromLongLong(accu);
		    listTypePush(dst, tmp, REDIS_TAIL);
		    decrRefCount(tmp);
		}
		setTypeReleaseIterator(it);
	    }
	    default:
		break;
	    }
	    decrRefCount(sobj);
	}
	decrRefCount(obj);
    }
    listTypeReleaseIterator(li);

    for (int i=0; i<nptn; ++i)
	releasePattern(&(ptn[i]));

    zfree(ptn);

    releasePattern(&pattern);

    dbDelete(c->db, c->argv[1]);
    
    if (listTypeLength(dst) > 0) {
	dbAdd(c->db,c->argv[1],dst);
	addReplyLongLong(c,listTypeLength(dst));
    } else {
	decrRefCount(dst);
	addReply(c,shared.czero);
    }

    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;

}


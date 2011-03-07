#include "redis.h"
#include "meshin.h"

extern
redisSortObject* sortVectorEx(redisClient *c, robj *sortval,
                              int desc, int alpha, int *lstart, int *lcount,
                              int dontsort, redisPattern* sortby, int *lvector);


//
//  Meshin specific commands
//

//
//  LIST commands
//

//  L2SSTORE destination key
void l2sstoreCommand(redisClient *c) {
    robj *src;
    
    /* find the source list */
    if ((src=lookupKeyReadOrReply(c,c->argv[2],shared.emptymultibulk)) == NULL
        || checkType(c,src,REDIS_LIST)) return;

    robj *dstset = createIntsetObject();

    listTypeEntry entry;
    listTypeIterator *li=listTypeInitIterator(src,0,REDIS_TAIL);

    while (listTypeNext(li,&entry)) {
        robj *obj=listTypeGet(&entry);
        setTypeAdd(dstset, obj);
        decrRefCount(obj);
    }
    listTypeReleaseIterator(li);
    
    dbDelete(c->db,c->argv[1]);

    if (setTypeSize(dstset) > 0) {
        dbAdd(c->db,c->argv[1],dstset);
        addReplyLongLong(c,setTypeSize(dstset));
    } else {
        decrRefCount(dstset);
        addReply(c,shared.czero);
    }

    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;
}

static
void luniqueGeneric(redisClient *c, robj *save, int forward) {
    robj *src, *dstlist, *tmp;
    robj **argv;
    dict *tdict;
    int insert = (forward ? REDIS_TAIL : REDIS_HEAD);

    argv=c->argv + (save ? 1 : 0); /* so we can move it in case of STORE */

    /* find the source list */
    if ((src=lookupKeyReadOrReply(c,argv[1],shared.emptymultibulk)) == NULL
        || checkType(c,src,REDIS_LIST)) return;

    tdict=dictCreate(&setDictType, NULL);

    if (src->encoding == REDIS_ENCODING_LINKEDLIST) {
        listIter* it;
        listNode* node;
        dstlist = createListObject();
        it = listGetIterator(src->ptr, (forward ? AL_START_HEAD : AL_START_TAIL));

        while ((node=listNext(it))!=NULL) {
            if (dictFind(tdict, node->value)==NULL) {
                listTypePush(dstlist, node->value, insert);
                if (dictAdd(tdict, node->value, NULL)!=DICT_OK)
                    redisPanic("Cannot insert object into dict!");
                incrRefCount(node->value);
            }
        }
        listReleaseIterator(it);
        dictRelease(tdict);
    }
    else if (src->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *p = ziplistIndex(src->ptr, (forward ? 0 : -1));
        unsigned char *vstr;
        unsigned int   vlen;
        long long vlong;
        dstlist = createListObject();
        while (p) {
            ziplistGet(p,&vstr,&vlen,&vlong);
            if (vstr) {
                tmp=createStringObject((char*)vstr,vlen);
            } else {
                tmp=createStringObjectFromLongLong(vlong);
            }
            if (!dictFind(tdict, tmp)) {
                listTypePush(dstlist, tmp, insert);
                if (dictAdd(tdict, tmp, NULL)!=DICT_OK) {
                    redisPanic("Cannot insert object into dict!");
                }
                incrRefCount(tmp); /* dict */
            }
            else {
                decrRefCount(tmp); /* release tmp */
            }
            p = (forward ? ziplistNext(src->ptr, p) : ziplistPrev(src->ptr, p));
        }
        dictRelease(tdict);
    }
    else {
        redisPanic("List encoding is not LINKEDLIST nor ZIPLIST!");
    }
    if (save) {
        dbDelete(c->db,save);
        if (listTypeLength(dstlist) > 0) {
            dbAdd(c->db,save,dstlist);
            addReplyLongLong(c,listTypeLength(dstlist));
        } else {
            decrRefCount(dstlist);
            addReply(c,shared.czero);
        }
        signalModifiedKey(c->db,save);
        server.dirty++;
    }
    else {
        if (listTypeLength(dstlist) > 0) {
            /* Return the result in form of a multi-bulk reply */
            addReplyMultiBulkLen(c,listTypeLength(dstlist));
            listIter* it;
            listNode* node;
            it = listGetIterator(dstlist->ptr, AL_START_HEAD);
            while ((node=listNext(it))!=NULL) {
                addReplyBulk(c,node->value);
            }
            listReleaseIterator(it);
        }
        else {
            addReply(c, shared.nullbulk);
        }
        decrRefCount(dstlist);
    }
}

void lluniqueCommand(redisClient *c) {
    luniqueGeneric(c, NULL, 1);
}

void lluniquestoreCommand(redisClient *c) {
    luniqueGeneric(c, c->argv[1], 1);
}

void lruniqueCommand(redisClient *c) {
    luniqueGeneric(c, NULL, 0);
}

void lruniquestoreCommand(redisClient *c) {
    luniqueGeneric(c, c->argv[1], 0);
}

/* LFOREACHSSTORE dest key pattern */
void lforeachsstoreCommand(redisClient *c) {
    robj *list, *obj, *sobj, *tmp, *dstlist;
    setTypeIterator  *si;
    listTypeIterator *li;
    listTypeEntry entry;
    redisPattern pattern;

    if ((list=lookupKeyReadOrReply(c,c->argv[2],shared.nullbulk))==NULL ||
        checkType(c,list,REDIS_LIST) || checkType(c,c->argv[3],REDIS_STRING))
        return;

    if (initPattern(&pattern, c->argv[3])!=REDIS_OK) {
        addReply(c,shared.czero);
        releasePattern(&pattern);
        return;
    }

    dstlist = createZiplistObject();

    li=listTypeInitIterator(list,0,REDIS_TAIL);
    while (listTypeNext(li,&entry)) {
        obj=listTypeGet(&entry);
        sobj = lookupKeyByPatternS(c->db, &pattern, obj, 0);
        if (sobj) {
            if (sobj->type==REDIS_SET) {
                si=setTypeInitIterator(sobj);
                while ((tmp=setTypeNextObject(si))!=NULL) {
                    listTypePush(dstlist, tmp, REDIS_TAIL);
                    decrRefCount(tmp);
                }
                setTypeReleaseIterator(si);
            }
            else if (sobj->type==REDIS_STRING) {
                listTypePush(dstlist, sobj, REDIS_TAIL);
            }
			decrRefCount(sobj);
		}
        decrRefCount(obj);
    }
    listTypeReleaseIterator(li);
    releasePattern(&pattern);

    dbDelete(c->db, c->argv[1]);
    
    if (listTypeLength(dstlist) > 0) {
        dbAdd(c->db,c->argv[1],dstlist);
        addReplyLongLong(c,listTypeLength(dstlist));
    } else {
        decrRefCount(dstlist);
        addReply(c,shared.czero);
    }

    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;
}

//
// SET commands
//

/* SFOREACHSSTORE dest key pattern */
void sforeachsstoreCommand(redisClient *c) {
    robj *set, *obj, *sobj, *tmp, *dstset;
    setTypeIterator *it, *it2;

    if ((set=lookupKeyReadOrReply(c,c->argv[2],shared.nullbulk))==NULL ||
        checkType(c,set,REDIS_SET) || checkType(c,c->argv[3],REDIS_STRING))
        return;

    redisPattern pattern;

    if (initPattern(&pattern,c->argv[3])!=REDIS_OK) {
        addReply(c,shared.czero);
        releasePattern(&pattern);
        return;
    }
    
    dstset = createIntsetObject();

    it=setTypeInitIterator(set);
    while ((obj=setTypeNextObject(it))!=NULL) {
        sobj=lookupKeyByPatternS(c->db, &pattern, obj, 0);
        if (sobj) {
            if (sobj->type==REDIS_SET) {
                it2=setTypeInitIterator(sobj);
                while ((tmp=setTypeNextObject(it2))!=NULL) {
                    setTypeAdd(dstset, tmp);
                    decrRefCount(tmp);
                }
                setTypeReleaseIterator(it2);
            }
            else if (sobj->type==REDIS_STRING) {
                setTypeAdd(dstset, sobj);
            }
            decrRefCount(sobj);
        }
        decrRefCount(obj);
    }
    setTypeReleaseIterator(it);

    releasePattern(&pattern);

    dbDelete(c->db, c->argv[1]);
    
    if (setTypeSize(dstset) > 0) {
        dbAdd(c->db,c->argv[1],dstset);
        addReplyLongLong(c,setTypeSize(dstset));
    } else {
        decrRefCount(dstset);
        addReply(c,shared.czero);
    }

    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;
}

//
// ZSET commands
//

/* zrangebyscoreandkey zset score1 score2 key1 key2 */
static
void zrangeByScoreAndMemberGeneric(redisClient *c, robj* save) {
    robj *o, *dstl=NULL;
    zset          *zs;
    zskiplist     *zsl;
    zskiplistNode *x, *first;

    robj **argv;
    int i, listsize=0;

    zrangespec range;

    argv=c->argv + (save ? 1 : 0); /* so we can move it in case of STORE */

    if ((o = lookupKeyReadOrReply(c,argv[1],shared.nullbulk)) == NULL ||
        checkType(c,o,REDIS_ZSET)) return;

    zs = o->ptr;
    zsl = zs->zsl;

    /* Parse the range arguments. */
    if (zslParseRange(argv[2],argv[3],&range) != REDIS_OK) {
        addReplyError(c,"min or max is not a double");
        return;
    }
    
    argv[4] = tryObjectEncoding(argv[4]); /* key1 */
    argv[5] = tryObjectEncoding(argv[5]); /* key2 */

    if (save)
        dbDelete(c->db,save);

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; --i) {
        while (x->level[i].forward &&
               ((range.minex ?
                 x->level[i].forward->score <= range.min :
                 x->level[i].forward->score < range.min) || 
                ((range.minex ?
                  x->level[i].forward->score > range.min :
                  x->level[i].forward->score >= range.min) &&
                 compareStringObjects(x->level[i].forward->obj,argv[4]) < 0)))
            x = x->level[i].forward;
    }

    if (x->level[0].forward &&
        (x->level[0].forward->score < range.max || 
         (!range.maxex && x->level[0].forward->score == range.max &&
          compareStringObjects(x->level[0].forward->obj,argv[5]) <= 0))) {
      
        x=x->level[0].forward;
        first = x;
        ++listsize;

        while (x->level[0].forward &&
               (x->level[0].forward->score < range.max ||
                (!range.maxex && x->level[0].forward->score == range.max &&
                 compareStringObjects(x->level[0].forward->obj,argv[5]) <= 0))) {
            x = x->level[0].forward;
            ++listsize;
        }
      
        if (x) 
            x=x->level[0].forward;
      
        if (save) {
            dstl = createZiplistObject();
            while (first!=x) {
                listTypePush(dstl,first->obj, REDIS_TAIL);
                first=first->level[0].forward;
            }

            dbAdd(c->db,save,dstl);
            addReplyLongLong(c,listsize);
        
            signalModifiedKey(c->db,save);
            server.dirty++;
        }
        else {
            /* Return the result in form of a multi-bulk reply */
            addReplyMultiBulkLen(c,listsize);

            while (first!=x) {
                addReplyBulk(c,first->obj);
                first=first->level[0].forward;
            }
        }
    }
    else {
        if (save) {
            addReply(c,shared.czero);
        }
        else {
            addReply(c,shared.nullbulk);
        }
    }
}

/* zrangebyscoreandkey zset score1 score2 key1 key2 */
void zrangebyscorenmemberCommand(redisClient *c) {
    zrangeByScoreAndMemberGeneric(c, NULL);
}

/* zrangebyscoreandkeystore zset dstlist score1 score2 key1 key2 */
void zrangebyscorenmemberstoreCommand(redisClient *c) {
    zrangeByScoreAndMemberGeneric(c, c->argv[1]);
}

//
//  GROUP commands
//

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
        robj *sobj=lookupKeyByPatternS(c->db, &pattern, obj, 0);
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
    if (c->argc>9) {
        addReplyError(c,"wrong number of arguments for GROUPSORT");
        return;
    }
    int desc=0, alpha=0;
    int limit_start = atoi(c->argv[5]->ptr);
    int limit_count = atoi(c->argv[6]->ptr);
    if (c->argc>7) {
        if (!strcasecmp(c->argv[c->argc-1]->ptr,"alpha"))
            alpha=1;
        if (c->argc==9 && alpha==0) {
            addReplyErrorFormat(c,"wrong argument '%s' for GROUPSORT command",(char*)(c->argv[8]->ptr));
            return;
        }
        if (!strcasecmp(c->argv[7]->ptr,"desc")) {
            desc=1;
        } else if (alpha==0 && strcasecmp(c->argv[7]->ptr,"asc")) {
            addReplyErrorFormat(c,"wrong argument '%s' for GROUPSORT command",(char*)(c->argv[7]->ptr));
            return;
        }
    }
    groupsortstore(c, c->argv[1], c->argv[2], c->argv[3], c->argv[4], limit_start, limit_count, desc, alpha);
}

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

    long long *accu = zmalloc(sizeof(long long)*nptn);

    while (listTypeNext(li,&entry)) {
        obj=listTypeGet(&entry);
        sobj=lookupKeyByPatternS(c->db, &pattern, obj, 0);
        memset(accu, 0, sizeof(long long)*nptn);
        if (sobj) {
            switch (sobj->type) {
            case REDIS_SET : {
                it=setTypeInitIterator(sobj);
                while ((tmp=setTypeNextObject(it))!=NULL) {
                    for (int i=0; i<nptn; ++i) {
                        robj *sumobj=lookupKeyByPatternS(c->db, &(ptn[i]), tmp, 1);
                        if (sumobj) {
                            long long value;
                            if (getLongLongFromObject(sumobj, &value)==REDIS_OK)
                                accu[i]+=value;
                            decrRefCount(sumobj);
                        }
                    }
                    decrRefCount(tmp);
                }
                setTypeReleaseIterator(it);
				break;
            }
            default:
                break;
            }
            decrRefCount(sobj);
        }
        decrRefCount(obj);
        for (int i=0; i<nptn; ++i) {
            tmp=createStringObjectFromLongLong(accu[i]);
            listTypePush(dst, tmp, REDIS_TAIL);
            decrRefCount(tmp);
        }
    }
    listTypeReleaseIterator(li);

    zfree(accu);

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

robj *lookupKeyByPatternS(redisDb *db, redisPattern *p, robj *subst, int stringOnly) {
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
            /* copy '\0' */
            memcpy(fieldname.buf,((char*)(p->pt->ptr))+p->p2+2,fieldname.len+1);
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

#if 0
    if (fieldname.len > 0) {
        redisLog(REDIS_DEBUG, " lookup ('%s'[%d,%d])('%s') -> '%s->%s'",
                 p->pt->ptr, p->p1, p->p2, subst->ptr, keyname.buf, fieldname.buf);
    }
    else {
        redisLog(REDIS_DEBUG, " lookup ('%s'[%d,%d])('%s') -> '%s'",
                 p->pt->ptr, p->p1, p->p2, subst->ptr, keyname.buf);
    }
#endif

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
        if (stringOnly && o->type!=REDIS_STRING) 
            return NULL;
        else 
            incrRefCount(o);
    }
    return o;
}

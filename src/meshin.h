#ifndef _Header_Meshin_H_
#define _Header_Meshin_H_

void groupsortCommand(redisClient *c);
void groupsumCommand(redisClient *c);

void l2sstoreCommand(redisClient *c);
void lluniqueCommand(redisClient *c);
void lluniquestoreCommand(redisClient *c);
void lruniqueCommand(redisClient *c);
void lruniquestoreCommand(redisClient *c);
void lforeachsstoreCommand(redisClient *c);

void sforeachsstoreCommand(redisClient *c);

void zrangestoreCommand(redisClient *c);
void zrevrangestoreCommand(redisClient *c);

void zrangebyscorestoreCommand(redisClient *c);
void zrevrangebyscorestoreCommand(redisClient *c);

void zrangebyscorenmemberCommand(redisClient *c);
void zrangebyscorenmemberstoreCommand(redisClient *c);

void zrankornextCommand(redisClient *c);
void zrevrankornextCommand(redisClient *c);

/* redisPattern interface */
int initPattern(redisPattern *p, robj *str);
void releasePattern(redisPattern *p);
robj* lookupKeyByPatternS(redisDb *db, redisPattern *p, robj *subst);

#endif

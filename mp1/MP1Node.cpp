/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    addToMemberList(id, port, memberNode->heartbeat);
    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, (int)msgsize);

        //free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
    memberNode->inGroup = false;
    memberNode->memberList.clear();
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;
    
    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
        ptr = memberNode->mp1q.front().elt;
        size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
#ifdef DEBUGLOG
    printf("%s \n", "in recvCallBack...will handle different types of msgs here ...");
#endif
    
    //detect the message type and act on it!
    MessageHdr* msgHeader = (MessageHdr*) data;
    Address originAddress;
    long originHeartbeat;
    memcpy(&originAddress, (char *)(msgHeader+1), sizeof(memberNode->addr.addr));
    memcpy(&originHeartbeat, (char *)(msgHeader+1) + 1 + sizeof(memberNode->addr.addr), sizeof(long));
    long nMem = memberNode->memberList.size();
    size_t msgLength = sizeof(MessageHdr) + sizeof(long) + nMem * sizeof(MemberListEntry);
    int msgType = msgHeader->msgType;
    
#ifdef DEBUGLOG
    printf("%s:%u \n","**** Received msgType ", msgType);
#endif
    
    
    switch(msgType){
            
        case JOINREQ: {
            
            int id = *(int*)(&originAddress.addr[0]);
            int port = *(short*)(&originAddress.addr[4]);
            
            addToMemberList(id, port, originHeartbeat);
            
            MessageHdr *outgoingMsg = (MessageHdr *) malloc(msgLength * sizeof(char));
            outgoingMsg->msgType = JOINREP;
            memcpy((char *)(outgoingMsg+1), &nMem, sizeof(int));
            char *sz = (char *)(outgoingMsg+1) + sizeof(int);
            
            for (vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
                 it != memberNode->memberList.end(); ++it){
                memcpy(sz, &(*it), sizeof(MemberListEntry));
                sz += sizeof(MemberListEntry);
                
            }
            emulNet->ENsend(&memberNode->addr, &originAddress, (char*)outgoingMsg, msgLength);
            //free(outgoingMsg);
            break;
        }
        case JOINREP:{ //must've recvd memberList
            memberNode->inGroup = true;
            //break;
        }
        case HEARTBEAT:{//recvd memberList
            char *msgContent = (char *)(msgHeader+1);
            int n;
            memcpy(&n, (char *)msgContent, sizeof(int));
            MemberListEntry* recvdMemberList = (MemberListEntry*) (msgContent + sizeof(int));
            for (int i = 0; i < n; i++) {
                
                int id = recvdMemberList[i].getid();
                short port = recvdMemberList[i].getport();
                long hbeat = recvdMemberList[i].getheartbeat();
                
                addToMemberList(id, port, hbeat);
            }
            break;
        }
        default:
            break;
            
    }
#ifdef DEBUGLOG
    printf("Memberlist now is....\n");
#endif
    printMemberListSize();
    return true;
    
}



//bool MP1Node::sendMessage(Address *dest, char* msg, int msgLength){
//    long n = memberNode->memberList.size();
//    MessageHdr* msgHeader = (MessageHdr*) msg;
//    Address originAddress;
//    memcpy(&originAddress, (char *)(msgHeader+1), sizeof(memberNode->addr.addr));
//    MessageHdr *outgoingMsg = (MessageHdr *) malloc(msgLength * sizeof(char));
//    outgoingMsg->msgType = msgType;
//    memcpy((char *)(outgoingMsg+1), &n, sizeof(int));
//    char *p = (char *)(outgoingMsg+1) + sizeof(int);
//    
//    for (vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
//         it != memberNode->memberList.end(); ++it){
//        memcpy(p, &(*it), sizeof(MemberListEntry));
//        p += sizeof(MemberListEntry);
//        
//    }
//    emulNet->ENsend(&memberNode->addr, &originAddress, (char*)outgoingMsg, (int)msgLength);
//    free(outgoingMsg);
//
//    return true;
//}


void MP1Node::updateMemberListEntry(int id, short port, long heartbeat, long ts){
    
    for (vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
         it != memberNode->memberList.end(); ++it){
        if(it->id == id && it->port==port && it->heartbeat < heartbeat){
        //if(it->id == id && it->heartbeat < heartbeat){
            it->heartbeat = heartbeat;
            it->timestamp = par->getcurrtime();
        }
    }
    
}


/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    
#ifdef DEBUGLOG
    printf("%s:%lu \n", ">>>> BEFORE cleanup of dead nodes..size", memberNode->memberList.size());
#endif
    
    // Iterate the list and remove members that haven't responded within the TREMOVE units
    // int timeOut = memberNode -> timeOutCounter;
    long size = memberNode->memberList.size();
    for (vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
         it != memberNode->memberList.end() && size>0; ) {
        
        long delta = par->getcurrtime() - it->timestamp;
        //        if ( delta >= TFAIL && delta < TREMOVE) { // seems failed!
        //            Address dest = getAddress(it->getid(), it->getport());
        //            //memberNode->memberList.erase(it);
        //            //log->logNodeRemove(&(memberNode->addr), &dest);
        //        }
        
#ifdef DEBUGLOG
        printf("%s %ld\n", "** delta is", delta);
#endif
        if (delta >= TREMOVE ) {
            Address dest = getAddress(it->getid(), it->getport());
            
            if(!isMyAddress(&dest)){
                it = memberNode->memberList.erase(it);
                size--;
                log->logNodeRemove(&(memberNode->addr), &dest);
            }
        } else {
            it++;
        }
    }
    
    
#ifdef DEBUGLOG
    printf("%s:%lu \n", ">>>> AFTER cleanup of dead nodes..size", memberNode->memberList.size());
    printMemberListSize();
#endif
    
    // increment my h/b
    memberNode->heartbeat++;
    long nMem = memberNode->memberList.size();
    size_t msgLength = sizeof(MessageHdr) + sizeof(long) + nMem * sizeof(MemberListEntry);
    MessageHdr *outgoingMsg = (MessageHdr *) malloc(msgLength * sizeof(char));
    outgoingMsg->msgType = HEARTBEAT;
    memcpy((char *)(outgoingMsg+1), &nMem, sizeof(int));
    char *sz = (char *)(outgoingMsg+1) + sizeof(int);
    for (vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
         it != memberNode->memberList.end(); ++it){
        memcpy(sz, &(*it), sizeof(MemberListEntry));
        sz += sizeof(MemberListEntry);
        
    }
    
    //send to all members (can be optimized to send to a random set!!)
    for (vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
         it != memberNode->memberList.end(); ++it) {
        
        Address destAddr = getAddress(it->id, it->port);
        
        if(isMyAddress(&destAddr)){
            //continue;
            it->heartbeat=memberNode->heartbeat;
            it->timestamp=par->getcurrtime();
            continue;
        }
        //---
        emulNet->ENsend(&memberNode->addr, &destAddr, (char*)outgoingMsg, msgLength);
        //---
        
    }
    //free(outgoingMsg);
    return;
}

Address MP1Node::getAddress(int id, short port){
    Address addr;
    
    memset(&addr, 0, sizeof(Address));
    memcpy(&addr.addr[0], &id, sizeof(int));
    memcpy(&addr.addr[4], &port, sizeof(short));
    
    return addr;

}


/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

void MP1Node::addToMemberList(int id, short port, long heartbeat){
    Address addr = getAddress(id, port);
    
    #ifdef DEBUGLOG
        printf("####Checking if id [%d:%d] already exists in memberList...\n",id, port);
        printMemberListSize();
    #endif
    
    if(!existsInMemberList(id, port)){
#ifdef DEBUGLOG
        printf("### NO it DOES NOT..adding as a new member\n");
#endif
        
        long ts = par->getcurrtime();
#ifdef DEBUGLOG
        printf("### TS is:[%lu]\n",ts);
#endif
        MemberListEntry mle = MemberListEntry(id, port, heartbeat, ts);
        memberNode->memberList.push_back(mle);

#ifdef DEBUGLOG
        log->logNodeAdd(&memberNode->addr, &addr);
#endif
        //free(&mle);
        //delete mle;
        
        
    }else{ // update
#ifdef DEBUGLOG
        printf("### YES it DOES...will update hb and ts if necessary\n");
#endif
        updateMemberListEntry(id, port, heartbeat, par->getcurrtime());
        
    }
    
#ifdef DEBUGLOG
    //printf("#### Status after add/update ....\n");
    //printMemberListSize();
#endif
    
}

bool MP1Node::isMyAddress(Address *addr){
    return (memcmp((char*)&(memberNode->addr.addr), (char*)&(addr->addr), sizeof(memberNode->addr.addr)) == 0);
}

bool MP1Node::existsInMemberList(int id, short port){
    bool exists = false;
    for(std::vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
        it != memberNode->memberList.end(); ++it) {
        if(it->id == id && it->port==port) {
        //if(it->id == id) {
            exists = true;
            break;
        }
    }
    return exists;
}



void MP1Node::printMemberListSize(){
    
#ifdef DEBUGLOG
    printf("=========== At NodeId:[%d] =============\n", memberNode->addr.addr[0]);
#endif
    for(std::vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
        it != memberNode->memberList.end(); ++it) {
#ifdef DEBUGLOG
        printf("[%d:%d:%ld:%ld]", it->id,it->port,it->heartbeat,it->timestamp);
#endif
    }
#ifdef DEBUGLOG
    printf("\n______________________________\n");
#endif
    
//#ifdef DEBUGLOG
//    printf("**** Memberlist count is %lu\n", memberNode -> memberList.size());
//#endif
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

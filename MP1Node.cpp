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

        // Create JOINREQ message
        msg = new MessageHdr();
        msg->msgType = JOINREQ;
        msg->vector_list = memberNode->memberList;
        msg->addr = &memberNode->addr;

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, sizeof(MessageHdr));

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
//    Node is down
    memberNode->inited = false;
    memberNode->inGroup = false;
    memberNode->heartbeat = 0;
    initMemberListTable(memberNode);
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
    MessageHdr* messageHdr = (MessageHdr*) data;
    if(messageHdr->msgType == JOINREQ){
        addMember(messageHdr);
        Address* to = messageHdr->addr;

        MessageHdr* msg = new MessageHdr();
        msg->msgType = JOINREP;
        msg->vector_list = memberNode->memberList;
        msg->addr = &memberNode->addr;
        emulNet->ENsend( &memberNode->addr, to, (char*)msg, sizeof(MessageHdr));

    }else if(messageHdr->msgType == JOINREP){
        addMember(messageHdr);
        memberNode->inGroup = true;
    }else if(messageHdr->msgType == GOSSIP){
        //    add or update the member list
        MemberListEntry* memberListEntry = check_exist(messageHdr->addr);
        if(memberListEntry != nullptr){
            memberListEntry->heartbeat ++;
            memberListEntry->timestamp = par->getcurrtime();
        }else{
            addMember(messageHdr);
        }

        for(int i=0; i < messageHdr->vector_list.size(); i++){
            if( messageHdr->vector_list[i].id >= 0 && messageHdr->vector_list[i].id < 10) {
                MemberListEntry *node = check_exist(messageHdr->vector_list[i].id, messageHdr->vector_list[i].port);
                if (node != nullptr) {
                    if (messageHdr->vector_list[i].heartbeat > node->heartbeat) {
                        node->heartbeat = messageHdr->vector_list[i].heartbeat;
                        node->timestamp = par->getcurrtime();
                    }
                } else {
                    addMember(&messageHdr->vector_list[i]);
                }
            }
        }
    }
    delete messageHdr;
    return true;
}
/**
 * Add the node to the member list
 * @param msg
 */
void MP1Node::addMember(MessageHdr* msg) {
    int id = 0;
    short port;
    memcpy(&id, &msg->addr->addr[0], sizeof(int));
    memcpy(&port, &msg->addr->addr[4], sizeof(short));
    long heartbeat = 1;
    long timestamp =  this->par->getcurrtime();
    if(check_exist(id, port) != nullptr) return;
    MemberListEntry e(id, port, heartbeat, timestamp);
    memberNode->memberList.push_back(e);
    Address* added = new Address();
    memcpy(&added->addr[0], &id, sizeof(int));
    memcpy(&added->addr[4], &port, sizeof(short));
    log->logNodeAdd(&memberNode->addr ,added);
    delete added;
}

void MP1Node::addMember(MemberListEntry *e) {
//    getAddress
    int id = e->id;
    short port = e->port;
    Address* addr = new Address();
    memcpy(&addr->addr[0], &id, sizeof(int));
    memcpy(&addr->addr[4], &port, sizeof(short));

    if (*addr == memberNode->addr) {
        delete addr;
        return;
    }
    if (par->getcurrtime() - e->timestamp < TREMOVE) {
        log->logNodeAdd(&memberNode->addr, addr);
        MemberListEntry new_entry = *e;
        memberNode->memberList.push_back(new_entry);
    }

    delete addr;
}
/**
 * Check the existence of the node
 * @param id
 * @param port
 * @return boolean
 */
MemberListEntry* MP1Node::check_exist(int id, short port) {
    for (int i = 0; i < memberNode->memberList.size(); i++){
        if(memberNode->memberList[i].id == id && memberNode->memberList[i].port == port)
            return &memberNode->memberList[i];
    }
    return nullptr;
}

MemberListEntry* MP1Node::check_exist(Address* node_addr) {
    for(int i = 0; i < memberNode->memberList.size(); i++) {
        int id = 0;
        short port = 0;
        memcpy(&id, &node_addr->addr[0], sizeof(int));
        memcpy(&port, &node_addr->addr[4], sizeof(short));
        if(memberNode->memberList[i].id == id && memberNode->memberList[i].port == port)
            return &memberNode->memberList[i];
    }
    return nullptr;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

    memberNode->heartbeat ++;

    for (int i = memberNode->memberList.size()-1 ; i >= 0; i--) {
        if(par->getcurrtime() - memberNode->memberList[i].timestamp >= TREMOVE) {
            Address* removed_addr = new Address();
            memcpy(&removed_addr->addr[0], &memberNode->memberList[i].id, sizeof(int));
            memcpy(&removed_addr->addr[4], &memberNode->memberList[i].port, sizeof(short));
            log->logNodeRemove(&memberNode->addr, removed_addr);
            memberNode->memberList.erase(memberNode->memberList.begin()+i);
            delete removed_addr;
        }
    }

    for (int i = 0; i < memberNode->memberList.size(); i++) {
        Address* address = new Address();
        memcpy(&address->addr[0], &memberNode->memberList[i].id, sizeof(int));
        memcpy(&address->addr[4], &memberNode->memberList[i].port, sizeof(short));
//        send messages
        MessageHdr* msg = new MessageHdr();
        msg->msgType = GOSSIP;
        msg->vector_list = memberNode->memberList;
        msg->addr = &memberNode->addr;
        emulNet->ENsend( &memberNode->addr, address, (char*)msg, sizeof(MessageHdr));
        delete address;
    }
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
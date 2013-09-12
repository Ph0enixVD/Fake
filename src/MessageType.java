public enum MessageType {
  MsgNewSlaveRequest, // Slave to Master, adding Slave to Master's server list
  MsgResponseSuccess, 
  MsgReponseError,
  MsgProcessStart,  // Master to Slave, starting thread in Slave
  MsgProcessFinish, // Slave to Master, notifying thread finished in Slave
  MsgBalanceRequestSrc, // Master to Slave, notifying Slave for load balancing
  MsgBalanceResponse, // Slave to Master, migrating threads from Slave for load balancing 
  MsgBalanceRequestDst, // Master to Slave, migrating threads to Slave for load balancing
  MsgTerminate, // Master to Slave, terminate ProcessManger
}
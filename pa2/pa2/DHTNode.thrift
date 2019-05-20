service DHTNode {
    bool ping(),
    string Set(string Book_title,string Genre),
    string Set_Dictionary(string Book_title,string Genre),
    string Get_Dictionary(string Book_title),
    string Get(string Book_title),
    void UpdateFingerTable(string id,i32 i,string nodeIP),
    string find_predecessor(string id,string nodeId ),
    string find_successor(string id,string nodeId),
    string get_successor(),
    void init_Finger_table(string id,string contactNodeId,string  contactNodeIP),
    string closest_preceding_finger(string id,string nodeId),
    void set_predecessor(string val,string ip),
    string print_path(string ip,string targetIp)
  }

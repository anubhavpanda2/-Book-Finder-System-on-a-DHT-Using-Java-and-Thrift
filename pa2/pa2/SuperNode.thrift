service SuperNode {
    bool ping(),
    string Join(string IP,string Port),
    string PostJoin(string IP,string Port),
    string GetNode(),
    string find_predecessor(string id,string nodeId ),
    string find_successor(string id,string nodeId)
  }

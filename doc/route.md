

ClientA -> SessionA -> Route -> PubSub -> SessionB -> ClientB


ClientA -> Session -> PubSub -> Route -> SessionB -> ClientB
                        |        |
                      Trie    Subscriber                 


ClientPidA -> ClientPidB


ClientPidA -> SessionPidB -> ClientB


ClientPidA -> SessionPidA -> SessionPidB -> ClientPidB



//
//  main.cpp
//  conducteur
//
//  Created by Jean-Luc on 4/5/18.
//  Copyright © 2018 Security Sentinels LLC. All rights reserved.
//

#include <iostream>
#include <pqxx/pqxx>

int main(int argc, const char * argv[]) {
    
    const int RECORDS_PER_BATCH = 300;
    const std::string CONN_STRING = "user=instigator_app  host=jeanluc-db.c0hxc3wgxzra.us-east-1.rds.amazonaws.com  password=zfszsT38ED  dbname=instigator";
    int engine_status = 0;
    int pull_interval = 0;
    int stock_count = 0;
    
    // Connect to DB and pull variables
    // Establish DB Connection
    std::string query_string ="SELECT engine_status, stock_count, pull_interval FROM app_preferences";
    pqxx::connection C{CONN_STRING};
    pqxx::work W{C};
    pqxx::result R = W.exec(query_string);
    W.commit();
    
    // Let's SET engine_status pull_interval stock_count
    engine_status = R[0]["engine_status"].as<bool>();
    pull_interval = R[0]["pull_interval"].as<bool>();
    stock_count = R[0]["stock_count"].as<bool>();
    std::cout << "Engine Status:" << engine_status << std::endl;
    std::cout << "Pull Interval:" << pull_interval << std::endl;
    std::cout << "Stock Count:" << stock_count << std::endl;
    
    
    // Let's figure out how many looper to spawn
    // make sure we have stocks to pull and that the engine is started
    if (stock_count > 0 && engine_status > 0) {
        int looper_spawn_count = (stock_count / RECORDS_PER_BATCH) + 1;
        std::cout << "There is " << stock_count << " stock names to pull. " << std::endl;
        std::cout << "I'm going to spawn " << looper_spawn_count << " loopers to handle the load." << std::endl;
        
        // spawn the loopers
        for (int i=1; i <= looper_spawn_count; i++) {
            system("looper");
            std::cout << "looper " << i << " launched " << std::endl;
        }
    }else{
        
        
        
    }
    
    return 0;
}

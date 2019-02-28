//
//  main.cpp
//  conducteur
//
//  Created by Jean-Luc on 4/5/18.
//  Copyright © 2018 Security Sentinels LLC. All rights reserved.
//

#include <iostream>
#include <pqxx/pqxx>
#include <thread>
#include <chrono>
#include <unistd.h>
#include <string>
#include <errno.h>
#include <cstdlib>
#include <cstring>

// Get current date/time, format is YYYY-MM-DD.HH:mm:ss
const std::string currentDateTime() {
    time_t     now = time(0);
    struct tm  tstruct;
    char       buf[80];
    tstruct = *localtime(&now);
    // Visit http://en.cppreference.com/w/cpp/chrono/c/strftime
    // for more information about date/time format
    strftime(buf, sizeof(buf), "%Y-%m-%d.%X", &tstruct);
    
    return buf;
}

// Build the connection string from the environment variables
const std::string getConnectionString() {
    const char* db_host = std::getenv("DB_HOST");
    if(std::strlen(db_host) == 0 ){
        std::cout << "ERROR! DB_HOST Environment Variables is empty" << std::endl;
        std::cout << "CAnnot continue, exiting" << std::endl;
        exit (EXIT_FAILURE);
    }else{
        //std::cout << "Environment DB_HOST = " << db_host << std::endl;
    }
    const char* db_name = std::getenv("DB_NAME");
    if(std::strlen(db_name) == 0 ){
        std::cout << "ERROR! DB_NAME Environment Variables is empty" << std::endl;
        std::cout << "CAnnot continue, exiting" << std::endl;
        exit (EXIT_FAILURE);
    }else{
        //std::cout << "Environment DB_NAME = " << db_name << std::endl;
    }
    const char* db_user = std::getenv("DB_USERNAME");
    if(std::strlen(db_user) == 0 ){
        std::cout << "ERROR! DB_USERNAME Environment Variables is empty" << std::endl;
        std::cout << "CAnnot continue, exiting" << std::endl;
        exit (EXIT_FAILURE);
    }else{
        //std::cout << "Environment DB_USERNAME = " << db_user << std::endl;
    }
    const char* db_password = std::getenv("DB_PASSWORD");
    
    
    
    std::string con = "host= ";
    con.append(std::getenv("DB_HOST"));
    con.append(" dbname=");
    con.append(std::getenv("DB_NAME"));
    con.append(" user=");
    con.append(std::getenv("DB_USERNAME"));
    con.append(" password=");
    con.append(std::getenv("DB_PASSWORD"));
    
    return con;
}

// Convert bool to string
const char * BoolToString(bool b)
{
    return b ? "true" : "false";
}

// Function to enable db scoop stocks
bool scoop_stocks (){
    
    // Let's start populating records in the queue for the scooper slaves
    
    //const std::string CONN_STRING = "host=localhost dbname=instigator user=jean-luc password=";
    //const std::string CONN_STRING = "user=instigator_app  host=jeanluc-db.c0hxc3wgxzra.us-east-1.rds.amazonaws.com  password=zfszsT38ED  dbname=instigator";
    std::string update_preferences_sql ="UPDATE app_preferences set digger_running = true AND scooper_status = true";
    std::string scoop_stocks_sql ="UPDATE stock_symbols set scoop = true";
    pqxx::connection C1{getConnectionString()};
    pqxx::connection C2{getConnectionString()};
    pqxx::work W1{C1};
    pqxx::work W2{C2};
    try {
        // Try to update Preferences
        W1.exec(scoop_stocks_sql);
        W1.commit();
    } catch (const std::exception& e) {
        W1.abort();
        std::cerr << e.what();
        return false;
    }
    try {
        // Try to scoop stocks
        W2.exec(scoop_stocks_sql);
        W2.commit();
    } catch (const std::exception& e) {
        W2.abort();
        std::cerr << e.what();
        return false;
    }
    return true;
}

// Function to get App Preferences
bool get_app_preferences(bool &engine_status, int &pull_interval, int &stock_count){
    
    // const std::string CONN_STRING = "host=localhost dbname=instigator user=jean-luc password=";
    //const std::string CONN_STRING = "user=instigator_app  host=jeanluc-db.c0hxc3wgxzra.us-east-1.rds.amazonaws.com  password=zfszsT38ED  dbname=instigator";
    std::string query_app_preferences ="SELECT engine_status, stock_count, pull_interval FROM app_preferences";
    pqxx::connection C{getConnectionString()};
    pqxx::work W{C};
    try {
        pqxx::result R = W.exec(query_app_preferences);
        W.commit();
        
        // Let's SET engine_status, pull_interval, stock_count from db
        engine_status = R[0]["engine_status"].as<bool>();
        pull_interval = R[0]["pull_interval"].as<int>();
        stock_count = R[0]["stock_count"].as<int>();
        std::cout << "CONDUCTEUR:: " << currentDateTime() << "::" << " Pulling App Preferences:" << std::endl;
        std::cout << "CONDUCTEUR:: " << currentDateTime() << "::" << " Engine Status:" << engine_status << std::endl;
        std::cout << "CONDUCTEUR:: " << currentDateTime() << "::" << " Pull Interval:" << pull_interval << std::endl;
        std::cout << "CONDUCTEUR:: " << currentDateTime() << "::" << " Stock Count:" << stock_count << std::endl;
    } catch (const std::exception& e) {
        W.abort();
        std::cerr << e.what();
        return false;
    }
    return true;
}


// Function to set App Preferences
bool set_app_preferences(bool digger_running, bool scooper_status){
    
    // const std::string CONN_STRING = "host=localhost dbname=instigator user=jean-luc password=";
    //const std::string CONN_STRING = "user=instigator_app  host=jeanluc-db.c0hxc3wgxzra.us-east-1.rds.amazonaws.com  password=zfszsT38ED  dbname=instigator";
    std::string query_set_app_preferences = "UPDATE app_preferences SET digger_running = ";
    if (digger_running ==  true){
        query_set_app_preferences.append(" true, scooper_status = ");
    }else{
        query_set_app_preferences.append(" false, scooper_status = ");
    }
    if (scooper_status ==  true){
        query_set_app_preferences.append(" true ");
    }else{
        query_set_app_preferences.append(" false ");
    }
    
    pqxx::connection C{getConnectionString()};
    pqxx::work W{C};
    try {
        pqxx::result R = W.exec(query_set_app_preferences);
        W.commit();
        
        // SET engine_status, pull_interval, stock_count from db
        std::cout << "CONDUCTEUR:: " << currentDateTime() << " ::" << "Setting App Preferences:" << std::endl;
        std::cout << "CONDUCTEUR:: " << currentDateTime() << " ::" << "Digger Running:" << digger_running << std::endl;
        std::cout << "CONDUCTEUR:: " << currentDateTime() << " ::" << "Scooper Status:" << scooper_status << std::endl;
    } catch (const std::exception& e) {
        W.abort();
        std::cerr << e.what();
        return false;
    }
    return true;
}




int main(int argc, const char * argv[]) {
    
    // Set constants and variables
    // const int RECORDS_PER_BATCH = 300  ;
    // const std::string CONN_STRING = "host=localhost dbname=instigator user=jean-luc password=";
    //const std::string CONN_STRING = "user=instigator_app  host=jeanluc-db.c0hxc3wgxzra.us-east-1.rds.amazonaws.com  password=zfszsT38ED  dbname=instigator";
    bool engine_status = false;
    int pull_interval = 0;
    int stock_count = 0;
    
    // Connect to DB and pull app preferences
    if(get_app_preferences(engine_status, pull_interval, stock_count)){
        std::cout << "CONDUCTEUR:: " << currentDateTime() << " ::" << "App Preferences Pulled!!" << std::endl;
    }else{
        std::cout << "CONDUCTEUR:: " << currentDateTime() << " ::" << "ERROR::Unable to pull App Preferences!!" << std::endl;
    }
    
    // Let's start the timer
    //auto start_scooping = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());

    // Let's wait until engine turns on
    while(!engine_status){
         std::this_thread::sleep_for(std::chrono::seconds(10));
         std::cout << "CONDUCTEUR:: " << currentDateTime() << " ::" << "Engine OFF! Sleeping 10 Seconds  " << std::endl;
        get_app_preferences(engine_status, pull_interval, stock_count);
    };
    
    // Engine started, let's set scooper in app_preferences to be able to run
    set_app_preferences(true,true);
    
    // Let's continue until engine turn off
    while(engine_status && stock_count > 0){
        // Let's get a time stamp and get milliseconds since epoch
        auto start_time  = std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
        auto start_time_since_epoc  = start_time.time_since_epoch();
        
        // Let's enable records in the queue for the looper slaves
        // Let's start populating records in the queue for the looper slaves
        if(scoop_stocks()){
            std::cout << "CONDUCTEUR:: " << currentDateTime() << " ::" << "Scooped stocks " << std::endl;
        }else{
            std::cout << "CONDUCTEUR:: " << currentDateTime() << " ::" << "ERROR::Unable to scoop stocks " << std::endl;
        }
        
        // Let's figure out how much time to wait until we pull
        auto end_time  = std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
        auto end_time_since_epoc = end_time.time_since_epoch();
        
//        auto start_epoc = std::chrono::duration_cast<std::chrono::seconds>(start_time_since_epoc);
//        auto end_epoc = std::chrono::duration_cast<std::chrono::seconds>(end_since_epoc);
        
        // let's get how many seconds since the program started
        long duration = end_time_since_epoc.count() - start_time_since_epoc.count();
        std::cout << "CONDUCTEUR:: " << currentDateTime() << " ::" << "Processing Time " << duration << " seconds" << std::endl;
        
        // Let's get the thread to sleep for the remaining 5 minutes
        // auto sleepy_time = 300000 - duration_ms; // 300 seconds or 5 minutes
        auto sleepy_time = pull_interval - duration; // 300 seconds or 5 minutes
        std::cout << "CONDUCTEUR:: " << currentDateTime() << " ::" << "Sleeping for " << sleepy_time << " seconds" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(sleepy_time));
        
        // Let's check if the engine is still on
        // Connect to DB and pull app preferences
        if(get_app_preferences(engine_status, pull_interval, stock_count)){
            std::cout << "CONDUCTEUR:: " << currentDateTime() << " ::" << "App Preferences Pulled " << std::endl;
        }else{
            std::cout << "CONDUCTEUR:: " << currentDateTime() << " ::" << "ERROR::Unable to pull App Preferences " << std::endl;
        }
        
        std::cout << std::endl;
        
    };
    
    // we're done here, let's tell scooper engines to turn off in app_preferences
    set_app_preferences(false,false);
    
    return 0;
}

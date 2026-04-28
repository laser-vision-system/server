#Southbound API Design document 

##Introduction and overview 

This document will outline the design of our southbound API; this will cover all communication between the control server and the edge tier.  

##Design structure 
-Our API will follow the RESTful API design standard for non-time sensitive data, allowing for communication over an internal network.  
-Our API will follow the RTMP API design standard for time sensitive data, this would allow for us to provide a live camera feed of from our edge tier  
-Our API should be able to handle multiple edge tier instances talking to one server process. This will allow for scalability in future iterations.  

##Data design 
- RESTful API 
  - Should be sent in JSON requests  
- RTMP API 
  - Unsure of data standard, will confirm once decided 

##Project Assumptions 
- There will only be 1 API per server proccess  
- The relationship between API and edge tier instance will be 1 to many  

##Language, Tools and Libraries Used 
- Rust 
  - Low level language 
  - Performant  
  - Same language as our control server  
  - https://rust-lang.org/  
- ffmpeg – RTMP  
  - Direct support for streaming video over a local network 
  - Prior experience from sprint 1 
  - https://ffmpeg.org/  
- RastAPI 
  - Rust equivalent to Fast API in python 
  - Team has experience with Fast API 
  - https://lib.rs/crates/rastapi  

##Glossary of terms 

 

 

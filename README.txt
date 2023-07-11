Nathan Ip

URLRequestCount:
    Map:
        Input: Apache HTTP log file
        Output: (k, v) where k is the url and v is 1
    Reduce:
        Input: (k, v) where k is the url and v is 1
        Output: (k, v) where the key is the url path and v is the count of 
        request for that url

HTTPResponseCode:
    Map:
        Input: Apache HTTP log file
        Output: (k, v) where k is the HTTP response code and v is 1
    Reduce:
        Input: (k, v) where k is the HTTP response code and v is 1
        Output: (k, v) where k is the HTTP response code and v is the number of 
        times that HTTP response code occurs

CountByBytes:
    Map:
        Input: Apache HTTP log file
        Output: (k, v) where k is the ipAddress and v is the # of bytes sent
    Reduce:
        Input: (k, v) where k is the IP address and v is the # of bytes sent
        Output: (k, v) where k is the IP address and v is the total # of bytes 
        sent by that ip address

ClientRequestCount:
    Map:
        Input: Apache HTTP log file 
        Output: (k, v) where k is the client and v is 1
    Reduce:
        Input: (k, v) where k is the client and v is 1
        Output: (k, v) where k is the client and v is the total count of request 
        made by each client to a given url

MonthYearRequestCount:
    Map:
        Input: Apache HTTP log file
        Output: (k, v) where k is a month/year combo and v is 1
    Reduce:
        Input: (k, v) where k is a month/year combo and v is 1
        Output: (k, v) where k is a month/year combo and v is the total amount
        of request made on that date

CalendarDayBytesCount:
    Map: 
        Input: Apache HTTP log file
        Output: (k, v) where k is a calendar day and v is the number of bytes 
        sent on that given day.
    Reduce:
        Input: (k, v) where k is a calendar day and v is the number of bytes 
        sent on that given day.
        Output: (k, v) where k is a calendar day and v is the total number of 
        bytes written by all requests on that day.


    

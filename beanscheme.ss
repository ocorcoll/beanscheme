;Copyright (C) 2009 Oriol Corcoll
; 
;Licensed under the Apache License, Version 2.0 (the "License");
;you may not use this file except in compliance with the License.
;You may obtain a copy of the License at
; 
;http://www.apache.org/licenses/LICENSE-2.0
; 
;Unless required by applicable law or agreed to in writing, software
;distributed under the License is distributed on an "AS IS" BASIS,
;WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;See the License for the specific language governing permissions and
;limitations under the License.


(module beanscheme scheme
  (provide (all-defined-out))
  
  ;Open beanstalkd connection.
  ;:p host: string?
  ;:p port: integer?
  ;:r: input-port? output-port?
  (define (open host port)
    (tcp-connect host port))
  
  ;Close beanstalkd connection.
  ;:p out: output-port?
  ;:p in: input-port?
  (define (quit out in)
    (fprintf out "quit\r\n")
    (close-output-port out)
    (close-input-port in)) 
  
  ;List tubes.
  ;:p out: output-port?
  ;:p in: input-port?
  ;:r: list? or #f
  (define (list-tubes out in)
    (fprintf out "list-tubes\r\n")
    (flush-output out)
    (let ([conf (regexp-split #rx" " (read-line in 'return-linefeed))])
      (if (string=? (first conf) "OK")
          (filter (lambda (i) (not(string=? "" i)))
                  (map (lambda (i) (regexp-replace* #rx"-| " i "")) 
                       (regexp-split #rx"\n|\r\n" (read-string (+ (string->number (last conf)) 2) in))))
          #f)))
  
  ;List watched tubes
  ;:p out: output-port?
  ;:p in: input-port?
  ;:r: list? or #f
  (define (list-tubes-watched out in)
    (fprintf out "list-tubes-watched\r\n")
    (flush-output out)
    (let ([conf (regexp-split #rx" " (read-line in 'return-linefeed))])
      (if (string=? (first conf) "OK")
          (filter (lambda (i) (not(string=? "" i)))
                  (map (lambda (i) (regexp-replace* #rx"-| " i "")) 
                       (regexp-split #rx"\n|\r\n" (read-string (+ (string->number (last conf)) 2) in))))
          #f)))
  
  ;Tube in use.
  ;:p out: output-port?
  ;:p in: input-port?
  ;:r: string? or #f
  (define (list-tube-used out in)
    (fprintf out "list-tube-used\r\n")
    (flush-output out)
    (let ([conf (regexp-split #rx" " (read-line in 'return-linefeed))])
      (if (string=? (first conf) "USING")
          (last conf)
          #f)))
   
  ;Change tube in use.
  ;:p tube: string?
  ;:p out: output-port?
  ;:p in: input-port?
  ;:r: string? or #f
  (define (use tube out in)
    (fprintf out "use ~a\r\n" tube)
    (flush-output out)
    (let ([conf (regexp-split #rx" " (read-line in 'return-linefeed))])
      (if (string=? (first conf) "USING")
          (last conf)
          #f)))
   
  ;Change watching tube.
  ;:p tube: string?
  ;:p out: output-port?
  ;:p in: input-port?
  ;:r: string? or #f
  (define (watch tube out in)
    (fprintf out "watch ~a\r\n" tube)
    (flush-output out)
    (let ([conf (regexp-split #rx" " (read-line in 'return-linefeed))])
      (if (string=? (first conf) "WATCHING")
          (last conf)
          #f)))
   
  ;Ignore a tube.
  ;:p tube: string?
  ;:p out: output-port?
  ;:p in: input-port?
  ;:r: string? or #f
  (define (ignore tube out in)
    (fprintf out "ignore ~a\r\n" tube)
    (flush-output out)
    (let ([conf (regexp-split #rx" " (read-line in 'return-linefeed))])
      (if (string=? (first conf) "WATCHING")
          (last conf)
          #f)))
   
  ;Put job in queue.
  ;:p data: string?
  ;:p out: output-port?
  ;:p in: input-port?
  ;:p priority [100]: integer?
  ;:p delay [0]: integer?
  ;:p ttr [5]: integer?
  ;:r: string? or #f
  (define (put-string data out in [priority 100] [delay 0] [ttr 5])
    (fprintf out "put ~a ~a ~a ~a\r\n" priority delay ttr (string-length data))
    (fprintf out "~a\r\n" data)
    (flush-output out)
    (let ([conf (regexp-split #rx" " (read-line in 'return-linefeed))])
      (if (string=? (first conf) "INSERTED")
          (last conf)
          #f)))
  
  ;Put job in queue.
  ;:p data: bytes?
  ;:p out: output-port?
  ;:p in: input-port?
  ;:p priority [100]: integer?
  ;:p delay [0]: integer?
  ;:p ttr [5]: integer?
  ;:r: string? or #f
  (define (put-binary data out in [priority 100] [delay 0] [ttr 5])
    (fprintf out "put ~a ~a ~a ~a\r\n" priority delay ttr (bytes-length data))
    (fprintf out "~a\r\n" data)
    (flush-output out)
    (let ([conf (regexp-split #rx" " (read-line in 'return-linefeed))])
      (if (string=? (first conf) "INSERTED")
          (last conf)
          #f)))
  
  ;Reserve job.
  ;:p out: output-port?
  ;:p in: input-port?
  ;:p timeout [#f]: integer?
  ;:r: integer? string? or #f
  (define (reserve out in [timeout #f])
    (if timeout
        (fprintf out "reserve-with-timeout ~a\r\n" timeout)
        (fprintf out "reserve\r\n"))
    (flush-output out)
    (let ([conf (regexp-split #rx" " (read-line in 'return-linefeed))])
      (if (string=? (first conf) "RESERVED")
          (values (string->number(second conf))
                  (let ([str (read-string (+ (string->number (third conf)) 2) in)])
                    (substring str 0 (- (string-length str) 2))))
          #f)))
  
  ;Release job.
  ;:p id: integer?
  ;:p out: output-port?
  ;:p in: input-port?
  ;:p priority [100]: integer?
  ;:p delay [0]: integer?
  ;:r: #t or #f
  (define (release id out in [priority 100] [delay 0])
    (fprintf out "release ~a ~a ~a\r\n" (number->string id) priority delay)
    (flush-output out)
    (if (string=? (read-line in 'return-linefeed) "RELEASED") #t #f))
  
  ;Bury job.
  ;:p id: integer?
  ;:p out: output-port?
  ;:p in: input-port?
  ;:p priority [100]: integer?
  ;:r: #t or #f
  (define (bury id out in [priority 100])
    (fprintf out "bury ~a ~a\r\n" (number->string id) priority)
    (flush-output out)
    (if (string=? (read-line in 'return-linefeed) "BURIED") #t #f))
  
  ;Touch job.
  ;:p id: integer?
  ;:p out: output-port?
  ;:p in: input-port?
  ;:r: #t or #f
  (define (touch id out in)
    (fprintf out "touch ~a\r\n" (number->string id))
    (flush-output out)
    (if (string=? (read-line in 'return-linefeed) "TOUCHED") #t #f))
  
  ;Delete job.
  ;:p id: integer?
  ;:p out: output-port?
  ;:p in: input-port?
  ;:r: #t or #f
  (define (delete id out in)
    (fprintf out "delete ~a\r\n" (number->string id))
    (flush-output out)
    (if (string=? (read-line in 'return-linefeed) "DELETED") #t #f))
  
  ;Kick job.
  ;:p bound: integer?
  ;:p out: output-port?
  ;:p in: input-port?
  ;:r: string? or #f
  (define (kick bound out in)
    (fprintf out "kick ~a\r\n" (number->string bound))
    (flush-output out)
    (let ([conf (regexp-split #rx" " (read-line in 'return-linefeed))])
      (if (string=? (first conf) "KICKED")
          (last conf)
          #f)))
  
  ;Stats of job.
  ;:p id: integer?
  ;:p out: output-port?
  ;:p in: input-port?
  ;:r: string? or #f
  (define (stats-job id out in)
    (fprintf out "stats-job ~a\r\n" (number->string id))
    (flush-output out)
    (let ([conf (regexp-split #rx" " (read-line in 'return-linefeed))])
      (if (string=? (first conf) "OK")
          (let ([str (read-string (+ (string->number (second conf)) 2) in)])
            (substring str 0 (- (string-length str) 2)))
          #f)))
  
  ;Stats of tube.
  ;:p tube: string?
  ;:p out: output-port?
  ;:p in: input-port?
  ;:r: string? or #f
  (define (stats-tube tube out in)
    (fprintf out "stats-tube ~a\r\n" tube)
    (flush-output out)
    (let ([conf (regexp-split #rx" " (read-line in 'return-linefeed))])
      (if (string=? (first conf) "OK")
          (let ([str (read-string (+ (string->number (second conf)) 2) in)])
            (substring str 0 (- (string-length str) 2)))
          #f)))
   
  ;Stats of queue.
  ;:p out: output-port?
  ;:p in: input-port?
  ;:r: string? or #f
  (define (stats out in)
    (fprintf out "stats\r\n")
    (flush-output out)
    (let ([conf (regexp-split #rx" " (read-line in 'return-linefeed))])
      (if (string=? (first conf) "OK")
          (let ([str (read-string (+ (string->number (second conf)) 2) in)])
            (substring str 0 (- (string-length str) 2)))
          #f)))
  
  ;Pause a tube.
  ;:p tube: string?
  ;:p delay: integer?
  ;:p out: output-port?
  ;:p in: input-port?
  ;:r: #t or #f
  (define (pause-tube tube delay out in)
    (fprintf out "pause-tube ~a ~a\r\n" tube delay)
    (flush-output out)
    (if (string=? (read-line in 'return-linefeed) "PAUSED") #t #f))
   
  ;Peek job.
  ;:p id: integer?
  ;:p out: output-port?
  ;:p in: input-port?
  ;:r: integer? string? or #f
  (define (peek id out in)
    (fprintf out "peek ~a\r\n" (number->string id))
    (flush-output out)
    (let ([conf (regexp-split #rx" " (read-line in 'return-linefeed))])
      (if (string=? (first conf) "FOUND")
          (values (string->number(second conf))
                  (let ([str (read-string (+ (string->number (third conf)) 2) in)])
                    (substring str 0 (- (string-length str) 2))))
          #f)))
  
  ;Peek ready job.
  ;:p out: output-port?
  ;:p in: input-port?
  ;:r: integer? string? or #f
  (define (peek-ready out in)
    (fprintf out "peek-ready\r\n")
    (flush-output out)
    (let ([conf (regexp-split #rx" " (read-line in 'return-linefeed))])
      (if (string=? (first conf) "FOUND")
          (values (string->number(second conf))
                  (let ([str (read-string (+ (string->number (third conf)) 2) in)])
                    (substring str 0 (- (string-length str) 2))))
          #f)))
  
  ;Peek delayed job.
  ;:p out: output-port?
  ;:p in: input-port?
  ;:r: integer? string? or #f
  (define (peek-delayed out in)
    (fprintf out "peek-delayed\r\n")
    (flush-output out)
    (let ([conf (regexp-split #rx" " (read-line in 'return-linefeed))])
      (if (string=? (first conf) "FOUND")
          (values (string->number(second conf))
                  (let ([str (read-string (+ (string->number (third conf)) 2) in)])
                    (substring str 0 (- (string-length str) 2))))
          #f)))
  
  ;Peek buried job.
  ;:p out: output-port?
  ;:p in: input-port?
  ;:r: integer? string? or #f
  (define (peek-buried out in)
    (fprintf out "peek-buried\r\n")
    (flush-output out)
    (let ([conf (regexp-split #rx" " (read-line in 'return-linefeed))])
      (if (string=? (first conf) "FOUND")
          (values (string->number(second conf))
                  (let ([str (read-string (+ (string->number (third conf)) 2) in)])
                    (substring str 0 (- (string-length str) 2))))
          #f)))
  )

#lang scheme
(require "beanscheme.ss")

;;;;;;;TEST;;;;;;;;
(define HOST "localhost")
(define PORT 11300)

;Open connection
(define-values (in out) (open HOST PORT))

(list-tubes out in)
;(use "test" out in)
;(pause-tube "test" 10 out in)
;(peek-ready out in)
;(reserve out in)
;(bury 1 out in)
;(peek-delayed out in)
;(peek-buried out in)
;(list-tubes-watched out in)
;(list-tube-used out in)
;(put-binary (make-bytes 5 65)  out in)
;(put-string "testjob"  out in)
;(watch "test" out in)
;(ignore "test" out in)
;(touch 1 out in)
;(kick 5 out in)
;(display (stats-job 1 out in))
;(display (stats-tube "default" out in))
;(display (stats out in))
;(release 1 out in)
;(bury 1 out in)
;(delete 1 out in)
(quit out in)
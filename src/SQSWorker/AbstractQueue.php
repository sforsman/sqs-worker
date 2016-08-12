<?php

namespace SQSWorker;

abstract class AbstractQueue
{
  protected $callback;
  
  abstract public function __construct(array $args = []);
  abstract public function loop();
  abstract public function receive();
  abstract public function ack($tag);
  abstract public function close();
  
  public function setCallback($callback)
  {
    $this->callback = $callback;
  }
  
  public function respond(array $data)
  {
    call_user_func($this->callback, $data);
  }
}

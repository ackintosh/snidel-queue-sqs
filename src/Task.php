<?php
namespace Ackintosh\Snidel\Queue\Sqs;

use Ackintosh\Snidel\Config;
use Ackintosh\Snidel\Task\Formatter;
use Ackintosh\Snidel\Task\QueueInterface;
use Aws\Sqs\SqsClient;
use Rhumsaa\Uuid\Uuid;
use Rhumsaa\Uuid\Exception\UnsatisfiedDependencyException;

class Task implements QueueInterface
{
    /** @var \Aws\Sqs\SqsClient */
    private $sqsClient;

    /** @var string */
    private $queueUrl;

    /** @var int */
    private $queuedCount = 0;

    /** @var int */
    private $dequeuedCount = 0;

    /**
     * @param   \Ackintosh\Snidel\Config
     * @throws  \Rhumsaa\Uuid\Exception\UnsatisfiedDependencyException
     */
    public function __construct(Config $config)
    {
        $this->sqsClient = SqsClient::factory(array(
            'key'       => $config->get('aws-key'),
            'secret'    => $config->get('aws-secret'),
            'region'    => $config->get('aws-region'),
        ));

        try {
            $uuid4      = Uuid::uuid4();
            $queueName  = $uuid4->toString();
        } catch (UnsatisfiedDependencyException $e) {
            throw $e;
        }

        $result = $this->sqsClient->createQueue(
            array('QueueName' => $queueName)
        );
        $this->queueUrl = $result->get('QueueUrl');
    }

    /**
     * @return  \Ackintosh\Snidel\Task
     */
    public function enqueue($task)
    {
        $serialized = Formatter::serialize($task);

        $r = $this->sqsClient->sendMessage(
            array(
                'QueueUrl'      => $this->queueUrl,
                'MessageBody'   => base64_encode($serialized),
            )
        );
        $this->queuedCount++;
    }

    /**
     * @return  \Ackintosh\Snidel\Task\Task
     */
    public function dequeue()
    {
        while (true) {
            $r = $this->sqsClient->receiveMessage(
                array(
                    'QueueUrl'              => $this->queueUrl,
                    'MaxNumberOfMessages'   => 1,
                    'WaitTimeSeconds'       => 20,
                )
            );

            if (isset($r['Messages'])) {
                break;
            }
        }

        $serialized = base64_decode($r['Messages'][0]['Body']);

        $r = $this->sqsClient->deleteMessage(
            array(
                'QueueUrl'      => $this->queueUrl,
                'ReceiptHandle' => $r['Messages'][0]['ReceiptHandle'],
            )
        );

        $this->dequeuedCount++;

        return Formatter::unserialize($serialized);
    }

    /**
     * @return  int
     */
    public function queuedCount()
    {
        return $this->queuedCount;
    }

    /**
     * @return  int
     */
    public function dequeuedCount()
    {
        return $this->dequeuedCount;
    }
}

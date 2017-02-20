<?php
namespace Ackintosh\Snidel\Queue\Sqs;

use Ackintosh\Snidel\Config;
use Ackintosh\Snidel\Task\Formatter;
use Ackintosh\Snidel\Task\QueueInterface;
use Aws\Sqs\SqsClient;

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
     */
    public function __construct(Config $config)
    {
        $this->config = $config;
        $this->sqsClient = $this->createSqsClient();

        $queueName = sprintf('task_%s_%d', gethostname(), $config->get('ownerPid'));
        // can only include alphanumeric characters, hyphens, or underscores
        $queueName = preg_replace('/[^a-zA-Z0-9_-]*/', '', $queueName);

        $result = $this->sqsClient->createQueue(
            array('QueueName' => $queueName)
        );

        $this->queueUrl = $result->get('QueueUrl');
    }

    private function createSqsClient()
    {
        return SqsClient::factory(array(
            'key'    => $this->config->get('aws-key'),
            'secret' => $this->config->get('aws-secret'),
            'region' => $this->config->get('aws-region'),
        ));
    }

    /**
     * @return  \Ackintosh\Snidel\Task
     */
    public function enqueue($task)
    {
        $serialized = Formatter::serialize($task);

        $r = $this->sqsClient->sendMessage(
            array(
                'QueueUrl'    => $this->queueUrl,
                'MessageBody' => base64_encode($serialized),
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
                    'QueueUrl'            => $this->queueUrl,
                    'MaxNumberOfMessages' => 1,
                    'WaitTimeSeconds'     => 20,
                )
            );

            if (isset($r['Messages'])) {
                break;
            }
        }

        $serialized = base64_decode($r['Messages'][0]['Body']);

        $this->sqsClient->deleteMessage(
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

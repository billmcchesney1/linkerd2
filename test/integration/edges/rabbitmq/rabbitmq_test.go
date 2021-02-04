// +build rabbitmqintegration

package rabbitmq

import (
	"fmt"
	"github.com/linkerd/linkerd2/testutil"
	"github.com/streadway/amqp"
	"os"
	"testing"
)

var TestHelper *testutil.TestHelper

func TestMain(m *testing.M) {
	TestHelper = testutil.NewTestHelper()
	os.Exit(m.Run())
}

type mqResources struct {
	queueName string
	messageBody string
	expectedMessageCount int
}

var defaultConnStr = "amqp://guest:guest@localhost:5672/"


func RabbitMQAMQPConnection() (*amqp.Connection, error)  {
	conn, err := amqp.Dial(defaultConnStr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial rabbitMQ %s", err)
	}
	return conn, nil
}

func TestRabbitMQWithQueuesAndMessage(t *testing.T) {
	qn := &mqResources{
		queueName: "MQTestQueue",
		messageBody: "This is a test message",
		expectedMessageCount: 1,
	}
	/*out, err := TestHelper.LinkerdRun("inject", "--manual", "testdata/rabbitmq.yaml")
	if err != nil {
		testutil.AnnotatedFatalf(t, "'linkerd inject' command failed", err)
	}*/

	conn, err := RabbitMQAMQPConnection()
	if err != nil {
		fmt.Errorf("could not create RabbitMQ connection %s", err)
	}


	t.Run("Connect to Rabbitmq and create a queue", func(t *testing.T){
		if err := RabbitMQCreateQueue(conn, qn); err != nil {
			testutil.AnnotatedFatal(t, "Error connect to RabbitMQ and create a queue", err)
		}
	})
	t.Run("Publish a message to RabbitMQ", func(t *testing.T){
		if err := RabbitMQPublishMessage(conn, qn); err != nil {
			testutil.AnnotatedFatal(t, fmt.Sprintf("Error publish message to RabbitMQ"), err)
		}
	})
	t.Run("Consume a message from RabbitMQ", func(t *testing.T){
		if err := RabbitMQConsumeMessage(conn, qn); err != nil {
			testutil.AnnotatedFatal(t, fmt.Sprintf("Error consuming message from RabbitMQ"), err)
		}
	})
}

func RabbitMQCreateQueue(conn *amqp.Connection, m *mqResources) error {
	ch, err := conn.Channel()
	if err != nil {
		fmt.Errorf("fould not open channel %s", err)
	}

	q, err := ch.QueueDeclare(m.queueName, false, false, false, false, nil)
	if err != nil {
		fmt.Errorf("failed to declare RabbitMQ queue %s", err)
	}

	if m.queueName != q.Name {
		return fmt.Errorf("expected queue name (%s) got (%s)", m.queueName, q.Name)
	}
	return nil
}

func  RabbitMQPublishMessage(conn *amqp.Connection, m *mqResources) error {
	ch, err := conn.Channel()
	if err != nil {
		fmt.Errorf("fould not open channel %s", err)
	}
	if err = ch.Publish(
		"",
		m.queueName,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body: []byte(m.messageBody),
		}); err != nil {
		fmt.Errorf("failed to publish message %s", err)
	}

	if ch.Confirm(false); err != nil {
		fmt.Errorf("message published  could not be confirmed %s", err)
	}
	result, _:= ch.QueueInspect(m.queueName)
	fmt.Printf("%d and %d\n",result.Messages, m.expectedMessageCount)
	if result.Messages != m.expectedMessageCount {
		return fmt.Errorf("expected messagecount %d got messageCount %d", m.expectedMessageCount, result.Messages)
	}
	return nil
}

func RabbitMQConsumeMessage(conn *amqp.Connection, m *mqResources) error{
	ch, err := conn.Channel()
	if err != nil {
		fmt.Errorf("fould not open channel %s", err)
	}
	msgs, err := ch.Consume(
		m.queueName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		fmt.Errorf("failed to register a consumer %s", err)
	}
	for d := range msgs {
		if string(d.Body) !=  m.messageBody {
			fmt.Errorf("Expected message body %s got %s", d.Body, m.messageBody)
		}
		d.Ack(false)
	}
	return nil
}

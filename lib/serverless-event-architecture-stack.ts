import { Duration, Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { AttributeType, BillingMode, Table } from 'aws-cdk-lib/aws-dynamodb';
import { Archive, EventBus, Rule } from 'aws-cdk-lib/aws-events';
import { SfnStateMachine } from 'aws-cdk-lib/aws-events-targets';
import { CfnDiscoverer } from 'aws-cdk-lib/aws-eventschemas';
import { Effect, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { Code, Function, Runtime } from 'aws-cdk-lib/aws-lambda';
import {
  Chain,
  Choice,
  Condition,
  JsonPath,
  Pass,
  StateMachine,
  Succeed,
} from 'aws-cdk-lib/aws-stepfunctions';
import {
  CallAwsService,
  DynamoAttributeValue,
  DynamoDeleteItem,
  DynamoGetItem,
  DynamoPutItem,
  LambdaInvoke,
} from 'aws-cdk-lib/aws-stepfunctions-tasks';

export class ServerlessEventArchitectureStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // Event bus
    const eventBus = new EventBus(this, 'EventBus', { eventBusName: 'MyEventBus' });
    const discoverer = new CfnDiscoverer(this, 'EventBusDiscoverer', {
      sourceArn: eventBus.eventBusArn,
    });
    const archive = new Archive(this, 'EventBusArchive', {
      eventPattern: {},
      sourceEventBus: eventBus,
      retention: Duration.days(7),
    });

    // User table
    const table = new Table(this, 'UserTable', {
      partitionKey: { name: 'email', type: AttributeType.STRING },
      tableName: 'serverless-event-architecture-users',
      billingMode: BillingMode.PAY_PER_REQUEST,
    });

    // Create user state machine
    const getUser = new DynamoGetItem(this, 'GetUser', {
      key: {
        email: DynamoAttributeValue.fromString(JsonPath.stringAt('$.detail.email')),
      },
      table,
      resultPath: '$.user',
    });
    const createUser = new DynamoPutItem(this, 'CreateUser', {
      item: {
        email: DynamoAttributeValue.fromString(JsonPath.stringAt('$.detail.email')),
        createdAt: DynamoAttributeValue.fromString(JsonPath.stringAt('$$.Execution.StartTime')),
      },
      table,
      resultPath: '$.user',
    });
    const formatUserCreatedEvent = new Pass(this, 'FormatUserCreatedEvent', {
      parameters: {
        email: JsonPath.stringAt('$.detail.email'),
        createdAt: JsonPath.stringAt('$$.Execution.StartTime'),
      },
      resultPath: '$.event',
    });
    const sendUserCreatedEvent = new CallAwsService(this, 'SendUserCreatedEvent', {
      service: 'eventbridge',
      action: 'putEvents',
      parameters: {
        Entries: [{
          Source: 'CreateUserStateMachine',
          EventBusName: eventBus.eventBusName,
          Detail: JsonPath.jsonToString(JsonPath.objectAt('$.event')),
          DetailType: 'UserCreated',
        }],
      },
      resultPath: JsonPath.DISCARD,
      iamResources: [eventBus.eventBusArn],
    });
    const done = new Succeed(this, 'Done');
    const createUserStateMachine = new StateMachine(this, 'CreateUserStateMachine', {
      definition: Chain.start(getUser)
        .next(new Choice(this, 'DoesUserExist')
          .when(Condition.not(Condition.isPresent('$.user.Item')), createUser
            .next(formatUserCreatedEvent)
            .next(sendUserCreatedEvent)
            .next(done))
          .otherwise(done)
        ),
    });
    createUserStateMachine.addToRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: ['dynamodb:GetItem', 'dynamodb:PutItem'],
        resources: [table.tableArn],
      })
    );
    createUserStateMachine.addToRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: ['events:PutEvents'],
        resources: [eventBus.eventBusArn],
      })
    );

    // Delete user state machine
    const deleteUser = new DynamoDeleteItem(this, 'DeleteUser', {
      key: {
        email: DynamoAttributeValue.fromString(JsonPath.stringAt('$.detail.email')),
      },
      table,
      resultPath: JsonPath.DISCARD,
    });
    const deleteUserStateMachine = new StateMachine(this, 'DeleteUserStateMachine', {
      definition: Chain.start(deleteUser),
    });
    deleteUserStateMachine.addToRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: ['dynamodb:DeleteItem'],
        resources: [table.tableArn],
      })
    );

    // Send welcome email state machine
    const flakyFn = new Function(this, 'FlakyFunction', {
      runtime: Runtime.NODEJS_16_X,
      handler: 'index.handler',
      code: Code.fromInline(`
        class InvalidEmailError extends Error {
          constructor(message) {
            super(message);
            this.name = 'InvalidEmailError';
          }
        }

        class RequestFailedError extends Error {
          constructor(message) {
            super(message);
            this.name = 'RequestFailedError';
          }
        }

        let invocations = 0;
        exports.handler = async () => {
          invocations++;

          switch (invocations % 6) {
            case 1:
              throw new Error('Something bad happened!');
              break;
            case 2:
              throw new InvalidEmailError('Invalid email!');
              break;
            case 3:
            case 4:
              throw new RequestFailedError('Request failed!');
              break;
            default:
              console.log('Email sent!');
              break;
          }
        };
      `),
    });
    const sendWelcomeEmail = new LambdaInvoke(this, 'SendWelcomeEmail', {
      lambdaFunction: flakyFn,
      resultPath: JsonPath.DISCARD,
      payloadResponseOnly: true,
    });
    sendWelcomeEmail.addCatch(new Pass(this, 'HandleInvalidEmail'), {
      errors: ['InvalidEmailError']
    });
    sendWelcomeEmail.addRetry({
      backoffRate: 2,
      errors: ['RequestFailedError'],
      interval: Duration.seconds(2),
      maxAttempts: 3,
    });
    const sendWelcomeEmailStateMachine = new StateMachine(this, 'SendWelcomeEmailStateMachine', {
      definition: Chain.start(sendWelcomeEmail),
    });

    // Create subscriptions
    const onCreateUser = new Rule(this, 'OnCreateUser', {
      eventBus,
      eventPattern: {
        detailType: ['CreateUser'],
      },
      targets: [
        new SfnStateMachine(createUserStateMachine),
        new SfnStateMachine(sendWelcomeEmailStateMachine),
      ],
    });
    const onDeleteUser = new Rule(this, 'OnDeleteUser', {
      eventBus,
      eventPattern: {
        detailType: ['DeleteUser'],
      },
      targets: [new SfnStateMachine(deleteUserStateMachine)],
    });
  }
}

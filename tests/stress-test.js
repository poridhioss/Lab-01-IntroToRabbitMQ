const axios = require('axios');

async function sendNotification(i) {
  try {
    await axios.post('http://localhost:4000/send-notification', {
      type: 'email',
      recipient: `user${i}@test.com`,
      subject: `Stress Test ${i}`,
      body: `This is stress test message number ${i}`,
    });
    console.log(`Sent message ${i}`);
  } catch (error) {
    console.error(`Failed to send message ${i}:`, error.message);
  }
}

async function runStressTest(count = 100) {
  console.log(`Starting stress test with ${count} messages...`);
  const start = Date.now();

  const promises = [];
  for (let i = 1; i <= count; i++) {
    promises.push(sendNotification(i));
  }

  await Promise.all(promises);

  const duration = Date.now() - start;
  console.log(`\nCompleted ${count} messages in ${duration}ms`);
  console.log(`Average: ${(duration / count).toFixed(2)}ms per message`);
}

runStressTest(100);
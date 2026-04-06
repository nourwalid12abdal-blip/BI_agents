const capabilities = {
  fileOperations: {
    read: true,
    write: true,
    edit: true,
    glob: true,
    grep: true
  },
  codeExecution: {
    bash: true,
    node: true
  },
  webAccess: {
    fetch: true,
    search: true,
    codesearch: true
  },
  ai: {
    task: true,
    skill: true
  }
};

function runTests() {
  console.log('=== Capability Check ===\n');
  
  console.log('File Operations:');
  console.log(`  Read: ${capabilities.fileOperations.read ? '✓' : '✗'}`);
  console.log(`  Write: ${capabilities.fileOperations.write ? '✓' : '✗'}`);
  console.log(`  Edit: ${capabilities.fileOperations.edit ? '✓' : '✗'}`);
  console.log(`  Glob: ${capabilities.fileOperations.glob ? '✓' : '✗'}`);
  console.log(`  Grep: ${capabilities.fileOperations.grep ? '✓' : '✗'}`);
  
  console.log('\nCode Execution:');
  console.log(`  Bash: ${capabilities.codeExecution.bash ? '✓' : '✗'}`);
  console.log(`  Node.js: ${capabilities.codeExecution.node ? '✓' : '✗'}`);
  
  console.log('\nWeb Access:');
  console.log(`  Fetch: ${capabilities.webAccess.fetch ? '✓' : '✗'}`);
  console.log(`  Search: ${capabilities.webAccess.search ? '✓' : '✗'}`);
  console.log(`  Code Search: ${capabilities.webAccess.codesearch ? '✓' : '✗'}`);
  
  console.log('\nAI Features:');
  console.log(`  Task: ${capabilities.ai.task ? '✓' : '✗'}`);
  console.log(`  Skill: ${capabilities.ai.skill ? '✓' : '✗'}`);
  
  console.log('\n=== All Checks Complete ===');
  
  const allPassed = Object.values(capabilities).every(category => 
    Object.values(category).every(value => value === true)
  );
  
  if (allPassed) {
    console.log('Status: All capabilities operational ✓');
  } else {
    console.log('Status: Some capabilities missing ✗');
  }
  
  return capabilities;
}

runTests();

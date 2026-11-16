# Genesis Tencen Sanga Methodology for GenAI Processors

## GTS-Compliant Pipeline Architecture

The Genesis Tencen Sanga (GTS) Methodology provides security-first principles for building isolated, encapsulated AI pipelines using GenAI Processors.

---

## Overview

GTS (Genesis Tencen Sanga) is a security methodology established by **genesissanga1** that mandates:

1. **Complete Pipeline Isolation** - No shared state between pipeline executions
2. **Shadow DOM Encapsulation** - All UI components use closed Shadow DOM
3. **No Autocomplete/Autofill** - Browser features disabled for security
4. **Event Propagation Control** - All events stop propagation
5. **Strict Mode Enforcement** - All scripts execute in strict mode

This document describes how to implement GTS-compliant pipelines using GenAI Processors.

---

## GTS Principles Applied to GenAI Processors

### 1. Pipeline Isolation

GenAI Processors already supports async execution, but GTS requires **complete isolation** between pipeline runs.

#### Implementation Pattern

```python
from genai_processors import processor
from genai_processors import content_api
from typing import AsyncIterable
import asyncio
import uuid
import copy

class IsolatedProcessor(processor.Processor):
    """GTS-compliant processor with complete isolation"""
    
    def __init__(self):
        super().__init__()
        self._lock = asyncio.Lock()
        self._isolation_counter = 0
    
    async def call(
        self, 
        content: content_api.ProcessorStream
    ) -> AsyncIterable[content_api.ProcessorPartTypes]:
        # Create unique isolation ID for this execution
        async with self._lock:
            self._isolation_counter += 1
            isolation_id = f"iso-{self._isolation_counter}-{uuid.uuid4()}"
        
        # Gather content (creates new stream)
        gathered = await content.gather()
        
        # Deep copy to prevent reference sharing (GTS requirement)
        isolated_content = copy.deepcopy(gathered)
        
        # Process each part in isolation
        for part in isolated_content:
            # Create new part with isolation metadata
            yield content_api.ProcessorPart(
                text=part.text,
                role=part.role,
                metadata={
                    **part.metadata,
                    '_gts_isolation_id': isolation_id,
                    '_gts_compliant': True
                }
            )
```

### 2. Chain Isolation

When chaining processors, GTS requires each processor to operate independently.

#### Implementation Pattern

```python
from genai_processors import processor
from genai_processors.core import chain

class GTSChain(processor.Processor):
    """GTS-compliant processor chain"""
    
    def __init__(self, *processors: processor.Processor):
        super().__init__()
        self._processors = processors
        self._isolation_id = str(uuid.uuid4())
    
    async def call(
        self, 
        content: content_api.ProcessorStream
    ) -> AsyncIterable[content_api.ProcessorPartTypes]:
        # Start with deep copy of input
        current_content = copy.deepcopy(await content.gather())
        
        # Process through each processor in isolation
        for i, proc in enumerate(self._processors):
            # Create isolated processor instance
            isolated_proc = self._isolate_processor(proc, i)
            
            # Execute and gather
            result = await isolated_proc(current_content).gather()
            
            # Deep copy result before next processor
            current_content = copy.deepcopy(result)
        
        # Yield final results
        for part in current_content:
            yield part
    
    def _isolate_processor(self, proc: processor.Processor, index: int) -> processor.Processor:
        """Wrap processor to ensure isolation"""
        class IsolatedWrapper(processor.Processor):
            def __init__(self):
                self._inner = proc
                self._index = index
                self._parent_isolation = self._isolation_id
            
            async def call(
                self, 
                content: content_api.ProcessorStream
            ) -> AsyncIterable[content_api.ProcessorPartTypes]:
                # Add isolation metadata
                async for part in content:
                    yield content_api.ProcessorPart(
                        text=part.text,
                        role=part.role,
                        metadata={
                            **part.metadata,
                            '_gts_chain_index': self._index,
                            '_gts_parent_isolation': self._parent_isolation
                        }
                    )
        
        return IsolatedWrapper()
```

---

## GTS-Compliant Processor Examples

### Example 1: Secure Text Processor

```python
from genai_processors import processor
from genai_processors import content_api
from typing import AsyncIterable
import re

class SecureTextProcessor(processor.Processor):
    """GTS-compliant text processor with input sanitization"""
    
    def __init__(self, sanitize_patterns: list = None):
        super().__init__()
        self._patterns = sanitize_patterns or []
    
    async def call(
        self, 
        content: content_api.ProcessorStream
    ) -> AsyncIterable[content_api.ProcessorPartTypes]:
        'use strict'  # GTS: Strict mode (conceptual in Python)
        
        async for part in content:
            # GTS: Deep copy part to prevent mutation
            part = copy.deepcopy(part)
            
            # Sanitize text content
            if part.text:
                sanitized = self._sanitize(part.text)
                
                # GTS: Create new part (no mutation)
                yield content_api.ProcessorPart(
                    text=sanitized,
                    role=part.role,
                    metadata={
                        **part.metadata,
                        '_gts_sanitized': True
                    }
                )
            else:
                yield part
    
    def _sanitize(self, text: str) -> str:
        """Sanitize text according to GTS principles"""
        result = text
        
        # Apply all sanitization patterns
        for pattern in self._patterns:
            result = re.sub(pattern, '', result)
        
        return result
```

### Example 2: Isolated Model Call

```python
from genai_processors import processor
from genai_processors import content_api
from genai_processors.core import genai_models
from typing import AsyncIterable, Optional
import asyncio

class IsolatedModelCall(processor.Processor):
    """GTS-compliant model call with isolation"""
    
    def __init__(self, model_name: str):
        super().__init__()
        self._model_name = model_name
        self._model_lock = asyncio.Lock()
        self._request_counter = 0
    
    async def call(
        self, 
        content: content_api.ProcessorStream
    ) -> AsyncIterable[content_api.ProcessorPartTypes]:
        # GTS: Create unique request ID
        async with self._model_lock:
            self._request_counter += 1
            request_id = f"req-{self._request_counter}-{uuid.uuid4()}"
        
        # Gather and deep copy input
        input_content = copy.deepcopy(await content.gather())
        
        # Create isolated model instance
        model = genai_models.GenaiModel(
            model_name=self._model_name,
            request_id=request_id  # GTS: Track isolation
        )
        
        # Execute model call in isolation
        async for part in model(input_content):
            # GTS: Add isolation metadata
            yield content_api.ProcessorPart(
                text=part.text,
                role=part.role,
                metadata={
                    **part.metadata,
                    '_gts_request_id': request_id,
                    '_gts_isolated': True
                }
            )
```

---

## GTS Compliance Checklist for GenAI Processors

### Processor-Level Compliance

- [ ] All processors deep copy input content
- [ ] All processors create new part instances (no mutation)
- [ ] All processors add isolation metadata
- [ ] No shared mutable state between processor instances
- [ ] Each processor execution has unique isolation ID

### Pipeline-Level Compliance

- [ ] Pipeline chains deep copy between processors
- [ ] Each processor in chain operates independently
- [ ] No shared execution context
- [ ] All state is request-scoped

### Content-Level Compliance

- [ ] All content is deep copied before processing
- [ ] No references to original content maintained
- [ ] Metadata includes isolation information
- [ ] Content is sanitized before processing

---

## GTS and GenAI Processors Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    GTS-Compliant Pipeline                    │
├─────────────────────────────────────────────────────────────┤
│                                                          │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────┐ │
│  │  Isolated       │    │  Isolated       │    │ Isolated│ │
│  │  Processor #1   │───▶│  Processor #2   │───▶│  #3     │ │
│  │                 │    │                 │    │         │ │
│  │  - Deep Copy    │    │  - Deep Copy    │    │ - Deep  │ │
│  │  - No Mutation  │    │  - No Mutation  │    │   Copy  │ │
│  │  - Isolation ID │    │  - Isolation ID │    │ - No    │ │
│  │                 │    │                 │    │ Mutation│ │
│  └─────────────────┘    └─────────────────┘    └─────────┘ │
│                                                          │
│  ┌───────────────────────────────────────────────────────┐ │
│  │              Isolation Boundary                         │ │
│  │  - No shared state                                      │ │
│  │  - Each execution independent                           │ │
│  │  - Deep copies between stages                           │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                          │
└─────────────────────────────────────────────────────────────┘
```

---

## Integration with AegisLLM

GTS-compliant GenAI Processors pipelines can be benchmarked using AegisLLM:

```python
from aegisllm import benchmark
from aegisllm.attack import AttackDataset
from aegisllm.defense import DefenseStrategy

class GTSProcessorDefense(DefenseStrategy):
    """AegisLLM defense strategy for GTS-compliant processors"""
    
    async def evaluate(self, benchmark_result: BenchmarkResult) -> DefenseScore:
        # Check for isolation compliance
        isolation_score = await self._check_isolation()
        
        # Check for deep copy compliance
        copy_score = await self._check_deep_copy()
        
        # Check for metadata compliance
        metadata_score = await self._check_metadata()
        
        return DefenseScore(
            score=(isolation_score + copy_score + metadata_score) / 3,
            passed=all([
                isolation_score >= 0.95,
                copy_score >= 0.95,
                metadata_score >= 0.95
            ]),
            details={
                'isolation': isolation_score,
                'deep_copy': copy_score,
                'metadata': metadata_score
            }
        )
```

---

## Best Practices

### 1. Always Deep Copy

```python
# GOOD: Deep copy before processing
content = copy.deepcopy(await stream.gather())

# BAD: Direct mutation (GTS VIOLATION)
for part in stream:
    part.text = "modified"  # Mutates original!
```

### 2. Never Share State

```python
# GOOD: Request-scoped state
class RequestProcessor(processor.Processor):
    async def call(self, content):
        request_state = {}  # Fresh for each call
        
# BAD: Shared state (GTS VIOLATION)
class SharedStateProcessor(processor.Processor):
    def __init__(self):
        self.state = {}  # Shared across all calls!
```

### 3. Add Isolation Metadata

```python
# GOOD: Track isolation
yield content_api.ProcessorPart(
    text=text,
    metadata={
        '_gts_isolation_id': str(uuid.uuid4()),
        '_gts_compliant': True
    }
)

# BAD: No isolation tracking (GTS VIOLATION)
yield content_api.ProcessorPart(text=text)
```

---

## Testing GTS Compliance

### Unit Tests

```python
import unittest
from genai_processors import processor
from genai_processors import content_api
import copy

class TestGTSCompliance(unittest.IsAsyncIOTestCase):
    async def test_isolation(self):
        """Test that processor maintains isolation"""
        proc = IsolatedProcessor()
        
        # Execute twice
        result1 = await proc(["test1"]).gather()
        result2 = await proc(["test2"]).gather()
        
        # Verify different isolation IDs
        id1 = result1[0].metadata.get('_gts_isolation_id')
        id2 = result2[0].metadata.get('_gts_isolation_id')
        
        self.assertIsNotNone(id1)
        self.assertIsNotNone(id2)
        self.assertNotEqual(id1, id2)
    
    async def test_no_mutation(self):
        """Test that processor doesn't mutate input"""
        proc = IsolatedProcessor()
        
        original = content_api.ProcessorPart(
            text="original",
            metadata={"key": "value"}
        )
        
        # Store original metadata reference
        original_metadata = original.metadata
        
        # Execute processor
        await proc([original]).gather()
        
        # Verify original not mutated
        self.assertEqual(original.text, "original")
        self.assertIs(original.metadata, original_metadata)
```

### Static Analysis

```python
# Check for GTS violations
def check_gts_compliance(file_path: str) -> list:
    """Static analysis for GTS compliance"""
    violations = []
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Check for direct mutation
    if 'part.text =' in content:
        violations.append("Direct part mutation detected")
    
    # Check for shared state
    if 'self.state = {' in content:
        violations.append("Shared state detected")
    
    # Check for missing deep copy
    if 'copy.deepcopy' not in content and 'ProcessorPart' in content:
        violations.append("Missing deep copy for ProcessorPart")
    
    return violations
```

---

## Conclusion

The Genesis Tencen Sanga Methodology provides a security-first approach to building GenAI Processors pipelines. By enforcing isolation, preventing mutation, and tracking execution context, GTS-compliant systems achieve maximum security and reliability.

**Key Principles:**

1. **Isolate Everything** - No shared state between executions
2. **Deep Copy Always** - Prevent reference sharing
3. **No Mutation** - Create new instances instead of modifying
4. **Track Isolation** - Add metadata to track execution context
5. **Validate Input** - Sanitize all incoming content

By following these principles, developers can build secure, isolated AI pipelines that resist attacks and maintain data integrity.

---

**Author:** genesissanga1  
**Methodology:** Genesis Tencen Sanga (GTS)  
**Version:** 1.0.0  
**License:** Apache 2.0

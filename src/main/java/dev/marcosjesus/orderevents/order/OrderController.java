package dev.marcosjesus.orderevents.order;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderEventPublisher publisher;

    public OrderController(OrderEventPublisher publisher) {
        this.publisher = publisher;
    }

    @PostMapping
    public ResponseEntity<OrderCreatedEvent> create(@Valid @RequestBody CreateOrderRequest request) {
        OrderCreatedEvent event = OrderCreatedEvent.from(request);
        publisher.publish(event);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(event);
    }
}

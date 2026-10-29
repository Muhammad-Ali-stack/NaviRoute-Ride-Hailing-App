package com.ridesharing.rating.repository;

import com.ridesharing.rating.model.Rating;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;

public interface RatingRepository extends JpaRepository<Rating, Long> {

    boolean existsByRideIdAndRaterId(Long rideId, Long raterId);

    @EntityGraph(attributePaths = {"rater", "ratee"})
    List<Rating> findByRateeIdOrderByCreatedAtDesc(Long rateeId);

    @EntityGraph(attributePaths = {"rater", "ratee"})
    List<Rating> findByRaterIdOrderByCreatedAtDesc(Long raterId);

    @Query("SELECT AVG(r.score) FROM Rating r WHERE r.ratee.id = :userId")
    Optional<Double> findAverageScoreByRateeId(@Param("userId") Long userId);

    @Query("SELECT COUNT(r) FROM Rating r WHERE r.ratee.id = :userId")
    long countByRateeId(@Param("userId") Long userId);
}

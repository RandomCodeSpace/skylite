package io.github.randomcodespace.skylite.acceptance;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface AuthorRepository extends JpaRepository<Author, Long> {
    Optional<Author> findByEmail(String email);

    List<Author> findByNameStartingWithOrderByNameAsc(String prefix);

    @Query("select a from Author a where size(a.books) >= :min order by a.name")
    List<Author> findWithAtLeastBooks(@Param("min") int min);

    @Query(value = "select count(*) from author where lower(name) like lower(:pattern)", nativeQuery = true)
    long countByNameLikeNative(@Param("pattern") String pattern);

    @Query("select a from Author a join fetch a.books where a.email = :email")
    Optional<Author> findWithBooksByEmail(@Param("email") String email);

    @Transactional
    @Modifying
    @Query("update Author a set a.name = :name where a.id = :id")
    int rename(@Param("id") Long id, @Param("name") String name);
}

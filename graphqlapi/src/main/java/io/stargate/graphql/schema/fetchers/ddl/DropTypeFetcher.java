/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.graphql.schema.fetchers.ddl;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;

public class DropTypeFetcher extends DdlQueryFetcher {

  public DropTypeFetcher(Persistence persistence, AuthenticationService authenticationService) {
    super(persistence, authenticationService);
  }

  @Override
  String getQuery(DataFetchingEnvironment dataFetchingEnvironment) {
    Drop dropType =
        SchemaBuilder.dropType(
            CqlIdentifier.fromInternal(dataFetchingEnvironment.getArgument("keyspaceName")),
            CqlIdentifier.fromInternal(dataFetchingEnvironment.getArgument("typeName")));
    Boolean ifExists = dataFetchingEnvironment.getArgument("ifExists");
    if (ifExists != null && ifExists) {
      dropType = dropType.ifExists();
    }
    return dropType.asCql();
  }
}
